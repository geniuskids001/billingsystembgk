const express = require("express");
const mysql = require("mysql2/promise");
const { Storage } = require("@google-cloud/storage");
const PDFDocument = require("pdfkit");

const app = express();
app.use(express.json({ limit: "2mb" }));

/* ================= CONFIGURATION ================= */
const config = {
  apiToken: process.env.API_TOKEN,
  database: {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    socketPath: process.env.DB_SOCKET_PATH,
    waitForConnections: true,
    connectionLimit: 10,
    timezone: "America/Mexico_City",
  },
  gcs: {
    bucket: process.env.GCS_BUCKET,
  },
  port: process.env.PORT || 8080,
  timezone: "America/Mexico_City",
};

/* ================= VALIDATION ================= */
function validateConfig() {
  const required = ["apiToken", "database.user", "database.password", "database.database", "gcs.bucket"];
  const missing = required.filter(key => {
    const value = key.split(".").reduce((obj, k) => obj?.[k], config);
    return !value;
  });
  
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
  }
}

validateConfig();

/* ================= MIDDLEWARE ================= */
function requireToken(req, res, next) {
  const token = req.headers["x-api-token"];
  
  if (!token) {
    return res.status(401).json({ ok: false, error: "Token requerido" });
  }
  
  if (token !== config.apiToken) {
    return res.status(401).json({ ok: false, error: "Token inválido" });
  }
  
  next();
}

function errorHandler(err, req, res, next) {
  console.error("Error:", err);
  
  const status = err.statusCode || 500;
  const message = err.message || "Error interno del servidor";
  
  res.status(status).json({ 
    ok: false, 
    error: message,
    ...(process.env.NODE_ENV === "development" && { stack: err.stack })
  });
}

/* ================= DATABASE ================= */
const pool = mysql.createPool(config.database);

async function getConnection() {
  try {
    const conn = await pool.getConnection();
    await conn.query(`SET time_zone = ?`, [config.timezone]);
    return conn;
  } catch (error) {
    console.error("Error al obtener conexión:", error);
    throw new Error("Error de conexión a la base de datos");
  }
}

async function executeInTransaction(callback) {
  const conn = await getConnection();
  
  try {
    await conn.beginTransaction();
    const result = await callback(conn);
    await conn.commit();
    return result;
  } catch (error) {
    await conn.rollback();
    throw error;
  } finally {
    conn.release();
  }
}

/* ================= GOOGLE CLOUD STORAGE ================= */
const storage = new Storage();
const bucket = storage.bucket(config.gcs.bucket);

async function deleteFileIfExists(path) {
  try {
    await bucket.file(path).delete();
  } catch (error) {
    // Archivo no existe, ignorar
    if (error.code !== 404) {
      console.warn(`Error al eliminar archivo ${path}:`, error.message);
    }
  }
}

async function uploadPdfToGCS(buffer, path) {
  const file = bucket.file(path);
  
  await file.save(buffer, {
    contentType: "application/pdf",
    resumable: false,
    metadata: { 
      cacheControl: "no-store",
      contentDisposition: `inline; filename="${path.split('/').pop()}"`,
    },
  });
  
  return `gs://${config.gcs.bucket}/${path}`;
}

/* ================= DATE HELPERS ================= */
function getCurrentDateInMX() {
  return new Date(
    new Date().toLocaleString("en-US", { timeZone: config.timezone })
  );
}

function getYearMonthDay() {
  const date = getCurrentDateInMX();
  return {
    year: date.getFullYear(),
    month: date.getMonth() + 1,
    day: date.getDate(),
  };
}

function generateCorteId(recibo) {
  const { year, month, day } = getYearMonthDay();
  const monthStr = String(month).padStart(2, "0");
  const dayStr = String(day).padStart(2, "0");
  
  return `${recibo.id_usuario}-${recibo.id_plantel}-${year}${monthStr}${dayStr}`;
}

/* ================= PATH HELPERS ================= */
function getReciboPdfPath(nombre) {
  return `recibos/${nombre}.pdf`;
}

function getCortePdfPath(nombre) {
  return `cortes/${nombre}.pdf`;
}

/* ================= PDF GENERATION ================= */
async function generateReciboPDF(recibo, detalles, opciones = {}) {
  const { cancelado = false } = opciones;
  
  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({ 
        size: "LETTER", 
        margin: 40,
        info: {
          Title: `Recibo ${recibo.id_recibo}`,
          Author: "Sistema BGK",
        }
      });
      
      const chunks = [];
      doc.on("data", chunk => chunks.push(chunk));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      doc.on("error", reject);

      // Header
      doc.fontSize(16).text("RECIBO", { align: "center" });
      
      if (cancelado) {
        doc.moveDown();
        doc.fontSize(22).fillColor("red").text("CANCELADO", { align: "center" });
        doc.fillColor("black");
      }

      // Información del recibo
      doc.moveDown();
      doc.fontSize(10)
        .text(`Folio: ${recibo.id_recibo}`)
        .text(`Alumno: ${recibo.id_alumno}`)
        .text(`Plantel: ${recibo.id_plantel}`)
        .text(`Fecha: ${new Date(recibo.fecha).toLocaleString("es-MX", { timeZone: config.timezone })}`);

      // Detalle
      doc.moveDown();
      doc.fontSize(12).text("Detalle de cobro", { underline: true });
      doc.moveDown(0.5);

      let subtotal = 0;
      detalles.forEach(detalle => {
        const precio = Number(detalle.precio_final);
        subtotal += precio;
        
        doc.fontSize(10).text(
          `${detalle.id_producto.padEnd(30)} $${precio.toFixed(2)}`
        );
      });

      // Total
      doc.moveDown();
      doc.fontSize(12).text(
        `TOTAL: $${Number(recibo.total_recibo).toFixed(2)}`,
        { align: "right", bold: true }
      );

      doc.end();
    } catch (error) {
      reject(error);
    }
  });
}

async function generateCortePDF(corte) {
  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({ 
        size: "LETTER", 
        margin: 40,
        info: {
          Title: `Corte ${corte.id_corte}`,
          Author: "Sistema BGK",
        }
      });
      
      const chunks = [];
      doc.on("data", chunk => chunks.push(chunk));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      doc.on("error", reject);

      // Header
      doc.fontSize(16).text("CORTE DE CAJA", { align: "center" });
      doc.moveDown();

      // Información del corte
      doc.fontSize(10)
        .text(`Corte: ${corte.id_corte}`)
        .text(`Plantel: ${corte.id_plantel}`)
        .text(`Usuario: ${corte.id_usuario}`)
        .text(`Fecha: ${corte.fecha}`);

      // Totales
      doc.moveDown();
      doc.fontSize(12).text("Totales por método de pago", { underline: true });
      doc.moveDown(0.5);

      const efectivo = Number(corte.total_efectivo);
      const tarjeta = Number(corte.total_tarjeta);
      const transferencia = Number(corte.total_transferencia);
      const gastos = Number(corte.gastos_efectivo);
      const total = Number(corte.total);

      doc.fontSize(10)
        .text(`Efectivo:       $${efectivo.toFixed(2)}`)
        .text(`Tarjeta:        $${tarjeta.toFixed(2)}`)
        .text(`Transferencia:  $${transferencia.toFixed(2)}`)
        .text(`Gastos efectivo: -$${gastos.toFixed(2)}`)
        .moveDown()
        .fontSize(12)
        .text(`TOTAL: $${total.toFixed(2)}`, { align: "right", bold: true });

      doc.end();
    } catch (error) {
      reject(error);
    }
  });
}

/* ================= BUSINESS LOGIC ================= */
async function calculateReciboTotal(conn, reciboId) {
  // Obtener recibo
  const [[recibo]] = await conn.execute(
    `SELECT * 
     FROM recibos 
     WHERE id_recibo = ? 
       AND status_recibo = 'Borrador'
     FOR UPDATE`,
    [reciboId]
  );
  if (!recibo) {
    throw new Error("Recibo no encontrado o no está en estado Borrador");
  }
  if (!recibo.fecha) {
    throw new Error("El recibo no tiene fecha operativa");
  }
  const fecha = new Date(recibo.fecha);
  const year = fecha.getFullYear();
  const month = fecha.getMonth() + 1;
  
  // Obtener detalles
  const [detalles] = await conn.execute(
    `SELECT * 
     FROM recibos_detalle
     WHERE id_recibo = ?
       AND status_detalle = 'Borrador'`,
    [reciboId]
  );
  if (detalles.length === 0) {
    throw new Error("El recibo no tiene detalles");
  }
  
  let totalRecibo = 0;
  
  // Calcular cada detalle
  for (const detalle of detalles) {
    const precioBase = Number(detalle.precio_base);
    let descuento = 0;
    let recargo = 0;
    let beca = 0;
    
    let reglas;
    
    if (detalle.frecuencia_producto === "Mensual") {
      // Lógica para productos Mensuales (mantiene comportamiento actual)
      let caso = "Corriente";
      if (detalle.mes && detalle.anio) {
        if (
          detalle.anio > year ||
          (detalle.anio === year && detalle.mes > month)
        ) {
          caso = "Adelantado";
        } else if (
          detalle.anio < year ||
          (detalle.anio === year && detalle.mes < month)
        ) {
          caso = "Vencido";
        }
      }
      
      // Obtener reglas con caso
      [reglas] = await conn.execute(
        `
        SELECT rp.*
        FROM reglas_producto rp
        JOIN reglas_producto_formas_pago rpfp
          ON rpfp.id_regla = rp.id_regla
        JOIN reglas_producto_casos rpc
          ON rpc.id_regla = rp.id_regla
        WHERE rp.id_producto = ?
          AND rpfp.forma_pago = ?
          AND rpc.caso = ?
          AND (rp.fecha_inicio IS NULL OR rp.fecha_inicio <= ?)
          AND (rp.fecha_fin IS NULL OR rp.fecha_fin >= ?)
          AND (
            rp.es_periodica = 0
            OR (
              rp.es_periodica = 1
              AND DAY(?) BETWEEN rp.dia_mes_inicio AND rp.dia_mes_fin
            )
          )
        ORDER BY rp.prioridad DESC
        `,
        [
          detalle.id_producto,
          recibo.forma_pago,
          caso,
          recibo.fecha,
          recibo.fecha,
          recibo.fecha
        ]
      );
    } else {
      // Lógica para productos Unica (sin caso)
      [reglas] = await conn.execute(
        `
        SELECT rp.*
        FROM reglas_producto rp
        JOIN reglas_producto_formas_pago rpfp
          ON rpfp.id_regla = rp.id_regla
        WHERE rp.id_producto = ?
          AND rpfp.forma_pago = ?
          AND (rp.fecha_inicio IS NULL OR rp.fecha_inicio <= ?)
          AND (rp.fecha_fin IS NULL OR rp.fecha_fin >= ?)
          AND (
            rp.es_periodica = 0
            OR (
              rp.es_periodica = 1
              AND DAY(?) BETWEEN rp.dia_mes_inicio AND rp.dia_mes_fin
            )
          )
        ORDER BY rp.prioridad DESC
        `,
        [
          detalle.id_producto,
          recibo.forma_pago,
          recibo.fecha,
          recibo.fecha,
          recibo.fecha
        ]
      );
    }
    
    // Aplicar reglas
    for (const regla of reglas) {
      if (regla.pct_descuento) {
        descuento += precioBase * regla.pct_descuento;
      }
      if (regla.pct_recargo) {
        recargo += precioBase * regla.pct_recargo;
      }
    }
    
    // Aplicar beca si es mensual
    if (detalle.frecuencia_producto === "Mensual") {
      const [[alumnoMensual]] = await conn.execute(
        `
        SELECT beca_monto
        FROM alumnos_mensuales
        WHERE id_alumno = ?
          AND id_producto = ?
        `,
        [recibo.id_alumno, detalle.id_producto]
      );
      const becaPct = Number(alumnoMensual?.beca_monto || 0);
      beca = precioBase * becaPct;
    }
    
    // Calcular precio final
    const montoAjuste = Number(detalle.monto_ajuste || 0);
    const precioCalculado =
      precioBase - descuento - beca + recargo + montoAjuste;
    const precioFinal = Math.max(0, Math.ceil(precioCalculado));
    
    // Guardar detalle
    await conn.execute(
      `
      UPDATE recibos_detalle
      SET descuento = ?,
          recargo = ?,
          beca = ?,
          precio_final = ?
      WHERE id_detalle = ?
      `,
      [descuento, recargo, beca, precioFinal, detalle.id_detalle]
    );
    
    totalRecibo += precioFinal;
  }
  
  // Actualizar total del recibo
  await conn.execute(
    `
    UPDATE recibos
    SET total_recibo = ?
    WHERE id_recibo = ?
    `,
    [totalRecibo, reciboId]
  );
  
  return { reciboId, total: totalRecibo };
}


/* ================= ENDPOINTS ================= */

// Calcular recibo
app.post("/calcular-recibo", requireToken, async (req, res, next) => {
  try {
    const { id_recibo } = req.body;
    
    if (!id_recibo) {
      return res.status(400).json({ ok: false, error: "id_recibo es requerido" });
    }

    const result = await executeInTransaction(async (conn) => {
      return await calculateReciboTotal(conn, id_recibo);
    });

    res.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

// Emitir recibo
app.post("/emitir-recibo", requireToken, async (req, res, next) => {
  const { id_recibo, nombre_recibo } = req.body;

  if (!id_recibo || !nombre_recibo) {
    return res.status(400).json({
      ok: false,
      error: "id_recibo y nombre_recibo son requeridos"
    });
  }

  let recibo;
  let corteId;

  try {
    await executeInTransaction(async (conn) => {

      // 1️⃣ Lock del recibo
      const [[row]] = await conn.execute(
        `
        SELECT *
        FROM recibos
        WHERE id_recibo = ?
          AND status_recibo = 'Borrador'
          AND (generando_pdf IS NULL OR generando_pdf = FALSE)
        FOR UPDATE
        `,
        [id_recibo]
      );

      if (!row) {
        throw new Error("Recibo no encontrado, no está en Borrador o está siendo procesado");
      }

      recibo = row;

      // 2️⃣ ANTIDUPLICADO MENSUAL (CLAVE)
      const [duplicados] = await conn.execute(
        `
        SELECT 1
        FROM recibos_detalle rd
        JOIN recibos r
          ON r.id_recibo = rd.id_recibo
        WHERE rd.frecuencia_producto = 'Mensual'
          AND rd.status_detalle = 'Emitido'
          AND r.status_recibo = 'Emitido'
          AND r.id_alumno = ?
          AND EXISTS (
            SELECT 1
            FROM recibos_detalle rd2
            WHERE rd2.id_recibo = ?
              AND rd2.frecuencia_producto = 'Mensual'
              AND rd2.id_producto = rd.id_producto
              AND rd2.mes = rd.mes
              AND rd2.anio = rd.anio
          )
        LIMIT 1
        `,
        [recibo.id_alumno, id_recibo]
      );

      if (duplicados.length > 0) {
        throw new Error(
          "Ya existe un recibo emitido para el mismo producto, mes y año"
        );
      }

      // 3️⃣ Emitir recibo
      corteId = generateCorteId(recibo);

      await conn.execute(
        `
        UPDATE recibos
        SET
          status_recibo = 'Emitido',
          encorte = ?,
          fecha_emision = NOW(),
          enimpresion = FALSE,
          generando_pdf = TRUE
        WHERE id_recibo = ?
        `,
        [corteId, id_recibo]
      );

      // 4️⃣ Emitir detalles
      await conn.execute(
        `
        UPDATE recibos_detalle
        SET status_detalle = 'Emitido'
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );

      // 5️⃣ Recalcular corte
      await conn.execute(
        `CALL sp_recalcular_corte(?)`,
        [corteId]
      );
    });

    // 6️⃣ Generar PDF
    const [detalles] = await pool.execute(
      `SELECT * FROM recibos_detalle WHERE id_recibo = ?`,
      [id_recibo]
    );

    const pdfBuffer = await generateReciboPDF(recibo, detalles);
    const pdfPath = getReciboPdfPath(nombre_recibo);

    await deleteFileIfExists(pdfPath);
    const rutaPdf = await uploadPdfToGCS(pdfBuffer, pdfPath);

    // 7️⃣ Guardar ruta PDF
    await pool.execute(
      `
      UPDATE recibos
      SET
        ruta_pdf = ?,
        generando_pdf = FALSE
      WHERE id_recibo = ?
      `,
      [rutaPdf, id_recibo]
    );

    res.json({
      ok: true,
      id_recibo,
      encorte: corteId,
      ruta_pdf: rutaPdf
    });

  } catch (error) {
    // Limpieza de lock técnico
    try {
      await pool.execute(
        `
        UPDATE recibos
        SET generando_pdf = FALSE
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );
    } catch (_) {}

    next(error);
  }
});

// Cancelar recibo
app.post("/cancelar-recibo", requireToken, async (req, res, next) => {
  const { id_recibo, nombre_recibo } = req.body;

  if (!id_recibo || !nombre_recibo) {
    return res.status(400).json({
      ok: false,
      error: "id_recibo y nombre_recibo son requeridos"
    });
  }

  let recibo;

  try {
    // =========================
    // TRANSACCIÓN
    // =========================
    await executeInTransaction(async (conn) => {

      // Lock del recibo
      const [[row]] = await conn.execute(
        `
        SELECT *
        FROM recibos
        WHERE id_recibo = ?
          AND status_recibo = 'Emitido'
        FOR UPDATE
        `,
        [id_recibo]
      );

      if (!row) {
        throw new Error("Recibo no encontrado o no está en estado Emitido");
      }

      recibo = row;

      // Cancelar recibo
      await conn.execute(
        `
        UPDATE recibos
        SET
          status_recibo = 'Cancelado',
          fecha_cancelacion = NOW()
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );
    });

    // =========================
    // GENERAR PDF CANCELADO
    // =========================
    const [detalles] = await pool.execute(
      `SELECT * FROM recibos_detalle WHERE id_recibo = ?`,
      [id_recibo]
    );

    const pdfBuffer = await generateReciboPDF(
      recibo,
      detalles,
      { cancelado: true }
    );

    const pdfPath = getReciboPdfPath(nombre_recibo);
    await deleteFileIfExists(pdfPath);
    const rutaPdf = await uploadPdfToGCS(pdfBuffer, pdfPath);

    // Guardar ruta del PDF cancelado
    await pool.execute(
      `
      UPDATE recibos
      SET ruta_pdf = ?
      WHERE id_recibo = ?
      `,
      [rutaPdf, id_recibo]
    );

    res.json({
      ok: true,
      id_recibo,
      ruta_pdf: rutaPdf
    });

  } catch (error) {
    next(error);
  }
});


// Generar PDF de corte
app.post("/cortes/generar-pdf", requireToken, async (req, res, next) => {
  try {
    const { idcorte, nombrecorte } = req.body;
    
    if (!idcorte || !nombrecorte) {
      return res.status(400).json({ 
        ok: false, 
        error: "idcorte y nombrecorte son requeridos" 
      });
    }

    const result = await executeInTransaction(async (conn) => {
      // Recalcular corte
      await conn.execute(`CALL sp_recalcular_corte(?)`, [idcorte]);

      // Obtener datos del corte
      const [[corte]] = await conn.execute(
        `SELECT * FROM cortes WHERE id_corte = ? FOR UPDATE`,
        [idcorte]
      );
      
      if (!corte) {
        throw new Error("Corte no encontrado");
      }

      return { corte };
    });

    // Generar y subir PDF
    const pdfBuffer = await generateCortePDF(result.corte);
    const pdfPath = getCortePdfPath(nombrecorte);
    
    await deleteFileIfExists(pdfPath);
    const rutaPdf = await uploadPdfToGCS(pdfBuffer, pdfPath);

    // Actualizar ruta del PDF
    await pool.execute(
      `UPDATE cortes SET ruta_pdf = ?, enimpresion = FALSE WHERE id_corte = ?`,
      [rutaPdf, idcorte]
    );

    res.json({ ok: true, ruta_pdf: rutaPdf });
  } catch (error) {
    next(error);
  }
});

// Health check
app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, status: "healthy", timestamp: new Date().toISOString() });
  } catch (error) {
    res.status(503).json({ ok: false, status: "unhealthy", error: error.message });
  }
});

// Error handler (debe ir al final)
app.use(errorHandler);

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM recibido, cerrando servidor...");
  await pool.end();
  process.exit(0);
});

// Start server
app.listen(config.port, () => {
  console.log(`🚀 BGK Backend ejecutándose en puerto ${config.port}`);
  console.log(`   Entorno: ${process.env.NODE_ENV || "development"}`);
  console.log(`   Timezone: ${config.timezone}`);
});
