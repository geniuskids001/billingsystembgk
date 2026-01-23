const express = require("express");
const mysql = require("mysql2/promise");
const { Storage } = require("@google-cloud/storage");
const PDFDocument = require("pdfkit");

const app = express();
app.use(express.json({ limit: "2mb" }));

// -------- Env --------
const API_TOKEN = process.env.API_TOKEN || "";
const DB_USER = process.env.DB_USER || "";
const DB_PASSWORD = process.env.DB_PASSWORD || "";
const DB_NAME = process.env.DB_NAME || "";
const DB_SOCKET_PATH = process.env.DB_SOCKET_PATH || ""; // /cloudsql/...
const GCS_BUCKET = process.env.GCS_BUCKET || "";

if (!API_TOKEN || !DB_USER || !DB_PASSWORD || !DB_NAME || !DB_SOCKET_PATH || !GCS_BUCKET) {
  console.warn("⚠️ Missing required env vars. Check API_TOKEN, DB_*, GCS_BUCKET.");
}

// -------- Auth middleware (simple token) --------
function requireToken(req, res, next) {
  const token = (req.headers["x-api-token"] || "").toString();
  if (!token || token !== API_TOKEN) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }
  next();
}

// -------- DB pool via Cloud SQL Unix socket --------
const pool = mysql.createPool({
  user: DB_USER,
  password: DB_PASSWORD,
  database: DB_NAME,
  socketPath: DB_SOCKET_PATH,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  // optional:
  // timezone: "Z",
});

const storage = new Storage();

// -------- Helpers --------
function ymNow() {
  const now = new Date();
  return { mes: now.getMonth() + 1, anio: now.getFullYear() };
}

function gcsPathForRecibo(idRecibo, fecha = new Date()) {
  const yyyy = fecha.getFullYear();
  const mm = String(fecha.getMonth() + 1).padStart(2, "0");
  return `recibos/${yyyy}/${mm}/recibo_${idRecibo}.pdf`;
}

async function uploadPdfBuffer(buffer, destinationPath) {
  const bucket = storage.bucket(GCS_BUCKET);
  const file = bucket.file(destinationPath);
  await file.save(buffer, {
    contentType: "application/pdf",
    resumable: false,
    metadata: {
      cacheControl: "no-store",
    },
  });
  // Ruta guardable: gs://...
  return `gs://${GCS_BUCKET}/${destinationPath}`;
}

async function renderReciboPdfSimple({ recibo, detalles }) {
  // PDF simple (placeholder). Puedes luego meter plantilla HTML si quieres.
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: "LETTER", margin: 40 });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    doc.fontSize(16).text("RECIBO", { align: "center" });
    doc.moveDown();

    doc.fontSize(11).text(`ID Recibo: ${recibo.id_recibo}`);
    doc.text(`Alumno: ${recibo.id_alumno}`);
    doc.text(`Plantel cobro: ${recibo.id_plantel}`);
    doc.text(`Fecha: ${new Date(recibo.fecha).toISOString()}`);
    doc.moveDown();

    doc.fontSize(12).text("Detalle:", { underline: true });
    doc.moveDown(0.5);

    let total = 0;
    for (const d of detalles) {
      const label = `${d.id_producto} ${d.frecuencia_producto}${d.mes ? ` ${d.mes}/${d.anio}` : ""}`;
      doc.fontSize(10).text(`${label}  -  $${Number(d.precio_final).toFixed(2)}`);
      total += Number(d.precio_final || 0);
    }
    doc.moveDown();
    doc.fontSize(12).text(`Total: $${Number(total).toFixed(2)}`, { align: "right" });

    doc.end();
  });
}

// =====================================================
// 1) CRON: Generar cargos mensuales
// POST /cron/generar-cargos
// =====================================================
app.post("/cron/generar-cargos", requireToken, async (req, res) => {
  const { mes, anio } = req.body?.mes && req.body?.anio ? req.body : ymNow();

  const conn = await pool.getConnection();
  try {
    // Inserta cargos faltantes del mes para alumnos activos y productos mensuales asignados.
    // Usa INSERT IGNORE para respetar UNIQUE uq_cargo (id_alumno,id_producto,mes,anio).
    const sql = `
      INSERT IGNORE INTO alumnos_cargos (
        id_cargo, id_alumno, id_producto, mes, anio, status_cargo, id_recibo, created_at, updated_at
      )
      SELECT
        UUID(), a.id_alumno, am.id_producto, ?, ?, 'Pendiente', NULL, NOW(), NOW()
      FROM alumnos a
      JOIN alumnos_mensuales am ON am.id_alumno = a.id_alumno
      JOIN productos p ON p.id_producto = am.id_producto
      WHERE a.status = 'Activo'
        AND p.frecuencia = 'Mensual'
    `;
    const [result] = await conn.execute(sql, [mes, anio]);

    res.json({
      ok: true,
      mes,
      anio,
      insertedApprox: result?.affectedRows ?? null,
      note: "INSERT IGNORE: si ya existía el cargo (UNIQUE), no lo duplica.",
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    conn.release();
  }
});

// =====================================================
// 2) Emitir recibo + “matar” cargos + generar PDF
// POST /recibos/emitir
// Body: { id_recibo: "..." }
// =====================================================
app.post("/recibos/emitir", requireToken, async (req, res) => {
  const idRecibo = (req.body?.id_recibo || "").toString().trim();
  if (!idRecibo) return res.status(400).json({ ok: false, error: "id_recibo requerido" });

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    // 1) Traer recibo
    const [recRows] = await conn.execute(
      `SELECT * FROM recibos WHERE id_recibo = ? FOR UPDATE`,
      [idRecibo]
    );
    if (recRows.length === 0) {
      await conn.rollback();
      return res.status(404).json({ ok: false, error: "Recibo no existe" });
    }
    const recibo = recRows[0];

    // Si ya está emitido y con pdf, idempotencia suave
    if (recibo.status_recibo === "Emitido" && recibo.ruta_pdf) {
      await conn.commit();
      return res.json({ ok: true, already: true, ruta_pdf: recibo.ruta_pdf });
    }

    // 2) Traer detalles
    const [detalles] = await conn.execute(
      `SELECT * FROM recibos_detalle WHERE id_recibo = ?`,
      [idRecibo]
    );

    // 3) “Matar” cargos (solo para detalles Mensual con mes/anio)
    // Regla: 1 detalle mensual corresponde a 1 cargo mensual por (alumno, producto, mes, anio)
    for (const d of detalles) {
      if (d.frecuencia_producto !== "Mensual") continue;
      if (!d.mes || !d.anio) continue;

      // Buscar cargo pendiente del alumno para ese producto/mes/año y bloquearlo
      const [cargoRows] = await conn.execute(
        `
        SELECT * FROM alumnos_cargos
        WHERE id_alumno = ?
          AND id_producto = ?
          AND mes = ?
          AND anio = ?
        FOR UPDATE
        `,
        [recibo.id_alumno, d.id_producto, d.mes, d.anio]
      );

      if (cargoRows.length === 0) {
        await conn.rollback();
        return res.status(409).json({
          ok: false,
          error: "Cargo no encontrado para detalle mensual",
          detail: { id_producto: d.id_producto, mes: d.mes, anio: d.anio },
        });
      }

      const cargo = cargoRows[0];

      // Validaciones simples
      if (cargo.status_cargo === "Cancelado") {
        await conn.rollback();
        return res.status(409).json({ ok: false, error: "Cargo cancelado, no se puede pagar", id_cargo: cargo.id_cargo });
      }
      if (cargo.status_cargo === "Pagado" && cargo.id_recibo && cargo.id_recibo !== idRecibo) {
        await conn.rollback();
        return res.status(409).json({ ok: false, error: "Cargo ya pagado en otro recibo", id_cargo: cargo.id_cargo });
      }

      // Marcar pagado y asociar recibo
      await conn.execute(
        `
        UPDATE alumnos_cargos
        SET status_cargo = 'Pagado',
            id_recibo = ?,
            updated_at = NOW()
        WHERE id_cargo = ?
        `,
        [idRecibo, cargo.id_cargo]
      );
    }

    // 4) Marcar recibo como emitido y set generando_pdf
    await conn.execute(
      `
      UPDATE recibos
      SET status_recibo = 'Emitido',
          generando_pdf = 1,
          fecha = COALESCE(fecha, NOW())
      WHERE id_recibo = ?
      `,
      [idRecibo]
    );

    await conn.commit();

    // 5) Generar PDF (fuera de transacción)
    // Volvemos a leer recibo/detalle (sin FOR UPDATE)
    const [recFinal] = await pool.execute(`SELECT * FROM recibos WHERE id_recibo = ?`, [idRecibo]);
    const [detFinal] = await pool.execute(`SELECT * FROM recibos_detalle WHERE id_recibo = ?`, [idRecibo]);
    const reciboFinal = recFinal[0];

    const pdfBuffer = await renderReciboPdfSimple({ recibo: reciboFinal, detalles: detFinal });

    const dest = gcsPathForRecibo(idRecibo, new Date(reciboFinal.fecha || Date.now()));
    const ruta = await uploadPdfBuffer(pdfBuffer, dest);

    await pool.execute(
      `
      UPDATE recibos
      SET ruta_pdf = ?,
          generando_pdf = 0
      WHERE id_recibo = ?
      `,
      [ruta, idRecibo]
    );

    res.json({ ok: true, id_recibo: idRecibo, ruta_pdf: ruta });
  } catch (e) {
    console.error(e);
    try { await conn.rollback(); } catch (_) {}
    // Best-effort: limpiar bandera si falló después de emitir
    try {
      await pool.execute(`UPDATE recibos SET generando_pdf = 0 WHERE id_recibo = ?`, [idRecibo]);
    } catch (_) {}
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    conn.release();
  }
});

// =====================================================
// 3) CRON: Reparar PDFs faltantes
// POST /cron/reparar-pdfs
// Body opcional: { limit: 50 }
// =====================================================
app.post("/cron/reparar-pdfs", requireToken, async (req, res) => {
  const limit = Number(req.body?.limit || 50);
  try {
    const [rows] = await pool.execute(
      `
      SELECT id_recibo
      FROM recibos
      WHERE status_recibo = 'Emitido'
        AND ruta_pdf IS NULL
        AND (generando_pdf = 0 OR generando_pdf IS NULL)
      ORDER BY fecha ASC
      LIMIT ?
      `,
      [limit]
    );

    const reparados = [];
    const fallos = [];

    for (const r of rows) {
      const idRecibo = r.id_recibo;

      try {
        // Reusar la misma lógica de pdf: generamos pdf y actualizamos ruta.
        const [recFinal] = await pool.execute(`SELECT * FROM recibos WHERE id_recibo = ?`, [idRecibo]);
        const [detFinal] = await pool.execute(`SELECT * FROM recibos_detalle WHERE id_recibo = ?`, [idRecibo]);
        const reciboFinal = recFinal[0];
        if (!reciboFinal) continue;

        // marcar bandera
        await pool.execute(`UPDATE recibos SET generando_pdf = 1 WHERE id_recibo = ?`, [idRecibo]);

        const pdfBuffer = await renderReciboPdfSimple({ recibo: reciboFinal, detalles: detFinal });
        const dest = gcsPathForRecibo(idRecibo, new Date(reciboFinal.fecha || Date.now()));
        const ruta = await uploadPdfBuffer(pdfBuffer, dest);

        await pool.execute(
          `UPDATE recibos SET ruta_pdf = ?, generando_pdf = 0 WHERE id_recibo = ?`,
          [ruta, idRecibo]
        );
        reparados.push({ id_recibo: idRecibo, ruta_pdf: ruta });
      } catch (e) {
        console.error("reparar-pdfs fallo", idRecibo, e);
        try { await pool.execute(`UPDATE recibos SET generando_pdf = 0 WHERE id_recibo = ?`, [idRecibo]); } catch (_) {}
        fallos.push({ id_recibo: idRecibo, error: e.message });
      }
    }

    res.json({ ok: true, encontrados: rows.length, reparados, fallos });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Healthcheck
app.get("/health", (req, res) => res.json({ ok: true }));

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`billing_service listening on :${port}`));
