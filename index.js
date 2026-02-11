const express = require("express");
const mysql = require("mysql2/promise");
const { Storage } = require("@google-cloud/storage");
const PDFDocument = require("pdfkit");
const path = require("path");

const app = express();
app.use(express.json({ limit: "2mb" }));

/* ================= LOGGER ================= */
const logger = {
  info: (msg, context = {}) => {
    console.log(JSON.stringify({
      level: 'INFO',
      timestamp: new Date().toISOString(),
      message: msg,
      ...context
    }));
  },
  warn: (msg, context = {}) => {
    console.warn(JSON.stringify({
      level: 'WARN',
      timestamp: new Date().toISOString(),
      message: msg,
      ...context
    }));
  },
  error: (msg, context = {}) => {
    console.error(JSON.stringify({
      level: 'ERROR',
      timestamp: new Date().toISOString(),
      message: msg,
      ...context
    }));
  }
};

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
  logger.error("Error handler triggered", {
    error_message: err.message,
    error_stack: err.stack,
    path: req.path
  });
  
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
    logger.error("Error al obtener conexión de BD", { error: error.message });
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

async function deleteFileIfExists(filepath) {
  try {
    await bucket.file(filepath).delete();
    logger.info("Archivo eliminado", { filepath });
  } catch (error) {
    if (error.code !== 404) {
      logger.warn("Error al eliminar archivo", { 
        filepath, 
        error: error.message 
      });
    }
  }
}

async function uploadPdfToGCS(buffer, filepath) {
  const file = bucket.file(filepath);
  
  await file.save(buffer, {
    contentType: "application/pdf",
    resumable: false,
    metadata: { 
      cacheControl: "no-store",
      contentDisposition: `inline; filename="${filepath.split('/').pop()}"`,
    },
  });
  
  const gsPath = `gs://${config.gcs.bucket}/${filepath}`;
  logger.info("PDF subido a GCS", { filepath, gsPath });
  return gsPath;
}

/* ================= DATE HELPERS ================= */
/**
 * Genera el ID de corte usando la fecha operativa del recibo
 * (NO depende del reloj del backend)
 * Formato: idUsuario-idPlantel-YYYYMMDD
 */
function generateCorteId(recibo) {
  if (!recibo.fecha) {
    throw new Error("El recibo no tiene fecha operativa para generar el corte");
  }

  const date = new Date(recibo.fecha);

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");

  return `${recibo.id_usuario}-${recibo.id_plantel}-${year}${month}${day}`;
}


/* ================= PATH HELPERS ================= */
function getReciboPdfPath(nombre) {
  return `recibos/${nombre}.pdf`;
}

function getCortePdfPath(nombre) {
  return `cortes/${nombre}.pdf`;
}

/* ================= PDF GENERATION ================= */
async function generateReciboPDF(recibo, detalles) {
  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({ 
        size: "LETTER", 
        margin: 50,
        bufferPages: true
      });
      const chunks = [];

      doc.on("data", c => chunks.push(c));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      doc.on("error", reject);

      const COLOR = "#00739A";
      const GRAY = "#666666";
      const LIGHT_GRAY = "#F8F9FA";
      const BORDER_GRAY = "#CCCCCC";
      const HIGHLIGHT_BG = "#E8F4F8"; // Color suave para resaltar total
      const logoPath = path.join(__dirname, "assets/businesslogo.png");

      /* ================= HEADER ================= */
      doc.image(logoPath, 50, 50, { width: 70 });

      doc
        .fillColor(COLOR)
        .fontSize(22)
        .font("Helvetica-Bold")
        .text("RECIBO DE PAGO", 200, 50, { align: "right" });

      // Validación de fecha
      if (!recibo.fecha_emision) {
        throw new Error("Recibo sin fecha de emisión - datos inconsistentes");
      }

      const fechaEmision = new Intl.DateTimeFormat("es-MX", {
        timeZone: "America/Mexico_City",
        weekday: "long",
        year: "numeric",
        month: "long",
        day: "numeric"
      }).format(new Date(recibo.fecha_emision));

      const fechaEmisionFormateada =
        fechaEmision.charAt(0).toUpperCase() + fechaEmision.slice(1);

      doc
        .fillColor(GRAY)
        .fontSize(9)
        .font("Helvetica")
        .text(`Folio: ${recibo.id_recibo || 'N/A'}`, 200, 80, { align: "right" })
        .text(`Fecha de emisión: ${fechaEmisionFormateada}`, 200, 93, {
          align: "right"
        })
        .text(`Forma de pago: ${recibo.forma_pago || 'N/A'}`, 200, 106, {
          align: "right"
        });

      doc
        .moveTo(50, 135)
        .lineTo(562, 135)
        .lineWidth(1.5)
        .stroke(COLOR);

      /* ============ WATERMARK CANCELADO ============ */
      if (recibo.status_recibo === "Cancelado") {
        doc.save();
        doc
          .opacity(0.15)
          .rotate(-35, { origin: [306, 400] })
          .lineWidth(4)
          .circle(306, 400, 200)
          .stroke("red");

        doc
          .fontSize(60)
          .font("Helvetica-Bold")
          .fillColor("red")
          .text("CANCELADO", 120, 370, {
            align: "center",
            width: 372
          });

        doc.restore();
        doc.opacity(1);
      }

      /* ================= ALUMNO ================= */
      doc
        .fillColor("#333333")
        .fontSize(9)
        .font("Helvetica")
        .text("DATOS DEL ALUMNO", 50, 155);

      const MAX_CHARS_NOMBRE = 80;
      const nombreAlumno = recibo.alumno_nombre_completo || 'Sin nombre';
      const nombreSeguro = nombreAlumno.length > MAX_CHARS_NOMBRE
        ? nombreAlumno.slice(0, MAX_CHARS_NOMBRE - 3) + "..."
        : nombreAlumno;

      doc
        .fontSize(11)
        .font("Helvetica-Bold")
        .text(nombreSeguro, 50, 170, { width: 512 });

      /* ================= CONCEPTOS - TABLA CENTRADA ================= */
      doc
        .fillColor("#333333")
        .fontSize(9)
        .font("Helvetica")
        .text("CONCEPTOS", 50, 210);

      // Tabla centrada: empieza en 85 en lugar de 50 (35px de margen extra)
      const tableLeft = 85;
      const tableWidth = 477; // Ancho reducido para centrar
      const tableTop = 230;

      // Header de tabla
      doc.rect(tableLeft, tableTop, tableWidth, 25).fill(LIGHT_GRAY);

      doc
        .fillColor(COLOR)
        .fontSize(9)
        .font("Helvetica-Bold")
        .text("CONCEPTO", tableLeft + 10, tableTop + 8, { width: 180 })
        .text("PRECIO", tableLeft + 195, tableTop + 8, { width: 50, align: "right" })
        .text("BECA", tableLeft + 250, tableTop + 8, { width: 45, align: "right" })
        .text("DESC.", tableLeft + 300, tableTop + 8, { width: 50, align: "right" })
        .text("RECARGO", tableLeft + 355, tableTop + 8, { width: 55, align: "right" })
        .text("TOTAL", tableLeft + 415, tableTop + 8, { width: 52, align: "right" });

      let y = tableTop + 35;
      doc.fontSize(9).font("Helvetica");

      if (!detalles || detalles.length === 0) {
        doc
          .fillColor(GRAY)
          .fontSize(10)
          .text("Sin conceptos registrados", tableLeft, y, {
            width: tableWidth,
            align: "center"
          });
        y += 30;
      } else {
        const MAX_CHARS_DESC = 35;
        
        detalles.forEach((d, index) => {
          // Fondo zebra
          if (index % 2 === 0) {
            doc.rect(tableLeft, y - 5, tableWidth, 20).fill("#FAFBFC");
          }

          const descripcionRaw = d.descripcion || 'Sin descripción';
          const descripcion = descripcionRaw.length > MAX_CHARS_DESC
            ? descripcionRaw.slice(0, MAX_CHARS_DESC - 3) + "..."
            : descripcionRaw;

          const precioBase = Number(d.precio_base) || 0;
          const beca = Number(d.beca) || 0;
          const descuento = Number(d.descuento) || 0;
          const recargo = Number(d.recargo) || 0;
          const precioFinal = Number(d.precio_final) || 0;

   
          doc
            .fillColor("#333333")
            .text(descripcion, tableLeft + 10, y, { width: 180 })
            .text(`$${precioBase.toFixed(2)}`, tableLeft + 195, y, {
              width: 50,
              align: "right"
            })
            .text(`$${beca.toFixed(2)}`, tableLeft + 250, y, {
              width: 45,
              align: "right"
            })
            .text(`$${descuento.toFixed(2)}`, tableLeft + 300, y, {
              width: 50,
              align: "right"
            })
            .text(`$${recargo.toFixed(2)}`, tableLeft + 355, y, {
              width: 55,
              align: "right"
            });

          // TOTAL con negrita y color
          doc
            .fillColor(COLOR)
            .font("Helvetica-Bold")
            .text(`$${precioFinal.toFixed(2)}`, tableLeft + 415, y, {
              width: 52,
              align: "right"
            });

          // Restaurar fuente para siguiente fila
          doc.font("Helvetica").fillColor("#333333");

          y += 20;
        });
      }

      // Línea divisoria después de conceptos
      y += 10;
      doc
        .moveTo(tableLeft, y)
        .lineTo(tableLeft + tableWidth, y)
        .lineWidth(0.5)
        .stroke(BORDER_GRAY);

      /* ================= TOTAL - ALINEADO A LA DERECHA ================= */
      y += 25;

      const totalBoxWidth = 212;
      const totalBoxLeft = 562 - totalBoxWidth; // Alineado a la derecha

      doc
        .rect(totalBoxLeft, y, totalBoxWidth, 55)
        .lineWidth(2)
        .fillAndStroke(LIGHT_GRAY, COLOR);

      doc
        .fillColor(GRAY)
        .fontSize(10)
        .font("Helvetica")
        .text("TOTAL PAGADO", totalBoxLeft + 10, y + 12, { width: 192 });

      const totalRecibo = Number(recibo.total_recibo) || 0;
      
      doc
        .fillColor(COLOR)
        .fontSize(22)
        .font("Helvetica-Bold")
        .text(
          `$${totalRecibo.toFixed(2)}`,
          totalBoxLeft + 10,
          y + 28,
          {
            width: 192,
            align: "right"
          }
        );

      /* ================= FOOTER ================= */
      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text(
          "Este documento es un comprobante de pago válido.",
          50,
          720,
          { 
            align: "center", 
            width: 512 
          }
        );

      doc.end();
      
    } catch (err) {
      reject(err);
    }
  });
}/* ================= BUSINESS LOGIC ================= */
async function calculateReciboTotal(conn, reciboId) {
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
  
  for (const detalle of detalles) {
    const precioBase = Number(detalle.precio_base);
    let descuento = 0;
    let recargo = 0;
    let beca = 0;
    
    let reglas;
    
    if (detalle.frecuencia_producto === "Mensual") {
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
    
    for (const regla of reglas) {
      if (regla.pct_descuento) {
        descuento += precioBase * regla.pct_descuento;
      }
      if (regla.pct_recargo) {
        recargo += precioBase * regla.pct_recargo;
      }
    }
    
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
    
    const montoAjuste = Number(detalle.monto_ajuste || 0);
    const precioCalculado =
      precioBase - descuento - beca + recargo + montoAjuste;
    const precioFinal = Math.max(0, Math.ceil(precioCalculado));
    
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

app.post("/calcular-recibo", requireToken, async (req, res, next) => {
  const startTime = Date.now();
  
  try {
    const { id_recibo } = req.body;
    
    if (!id_recibo) {
      return res.status(400).json({ ok: false, error: "id_recibo es requerido" });
    }

    logger.info("Iniciando cálculo de recibo", { id_recibo });

    const result = await executeInTransaction(async (conn) => {
      return await calculateReciboTotal(conn, id_recibo);
    });

    const duration = Date.now() - startTime;
    logger.info("Recibo calculado exitosamente", { 
      id_recibo, 
      total: result.total,
      duration_ms: duration 
    });

    res.json({ ok: true, ...result });
  } catch (error) {
    logger.error("Error al calcular recibo", {
      id_recibo: req.body.id_recibo,
      error: error.message,
      duration_ms: Date.now() - startTime
    });
    next(error);
  }
});

// ============================================================================
// EMITIR RECIBO - Versión transaccional robusta
// ============================================================================
app.post("/emitir-recibo", requireToken, async (req, res, next) => {
  const { id_recibo, nombre_recibo } = req.body;
  const startTime = Date.now();

  if (!id_recibo || !nombre_recibo) {
    return res.status(400).json({
      ok: false,
      error: "id_recibo y nombre_recibo son requeridos"
    });
  }

  logger.info("Iniciando emisión de recibo", { id_recibo, nombre_recibo });

  // ============================================================================
  // VARIABLES DE RESULTADO (un solo punto de salida)
  // ============================================================================
  let resultado = null;
  let errorFinal = null;

  try {
    // ========================================================================
    // FASE 1: TRANSACCIÓN
    // ========================================================================
    const txResult = await executeInTransaction(async (conn) => {
      
     const [rows] = await conn.execute(
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

const row = rows[0];

if (!row) {
  throw new Error("Recibo no encontrado, no está en Borrador o está siendo procesado");
}


      if (!row.id_alumno || !row.id_plantel || !row.fecha) {
        throw new Error("Recibo con datos incompletos (alumno, plantel o fecha faltante)");
      }

      if (Number(row.total_recibo) < 0) {
        throw new Error("El recibo debe tener un total igual o mayor a cero");
      }

      const [[detallesCount]] = await conn.execute(
        `
        SELECT COUNT(*) as total
        FROM recibos_detalle
        WHERE id_recibo = ?
          AND status_detalle = 'Borrador'
        `,
        [id_recibo]
      );

      if (detallesCount.total === 0) {
        throw new Error("El recibo no tiene detalles válidos para emitir");
      }

      const [duplicados] = await conn.execute(
        `
        SELECT 1
        FROM recibos_detalle rd
        JOIN recibos r ON r.id_recibo = rd.id_recibo
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
        [row.id_alumno, id_recibo]
      );

      if (duplicados.length > 0) {
        throw new Error(
          "Ya existe un recibo emitido para el mismo producto, mes y año"
        );
      }

      const corteId = generateCorteId(row);

      const [updateRecibo] = await conn.execute(
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

if (updateRecibo.affectedRows !== 1) {
  logger.error("ERROR CRÍTICO: No se pudo actualizar recibo al emitir", {
    id_recibo,
    corteId,
    affectedRows: updateRecibo.affectedRows
  });

  throw new Error(
    `ERROR CRÍTICO: UPDATE recibos no afectó filas (affectedRows=${updateRecibo.affectedRows})`
  );
}

logger.info("Recibo actualizado a Emitido correctamente", {
  id_recibo,
  corteId
});


const [debugRows] = await conn.execute(
  `
  SELECT
    status_recibo,
    fecha_emision,
    encorte,
    generando_pdf
  FROM recibos
  WHERE id_recibo = ?
  `,
  [id_recibo]
);

logger.error("DEBUG POST-UPDATE RECIBO (MISMA TX)", {
  id_recibo,
  row: debugRows[0]
});


      await conn.execute(
        `
        UPDATE recibos_detalle
        SET status_detalle = 'Emitido'
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );

      await conn.execute(
        `CALL sp_recalcular_corte(?)`,
        [corteId]
      );

      logger.info("Transacción de emisión completada", { 
        id_recibo, 
        corteId 
      });

      return {
        id_recibo,
        corteId,
        id_alumno: row.id_alumno,
        id_plantel: row.id_plantel
      };
    });

    // ========================================================================
    // FASE 2: VERIFICACIÓN POST-COMMIT
    // ========================================================================
  const [rowsEmitido] = await pool.execute(
  `
  SELECT *
  FROM recibos
  WHERE id_recibo = ?
    AND status_recibo = 'Emitido'
    AND encorte = ?
  `,
  [txResult.id_recibo, txResult.corteId]
);

const reciboEmitido = rowsEmitido[0];


    if (!reciboEmitido) {
      throw new Error(
        "ERROR CRÍTICO: La transacción reportó éxito pero el recibo no está en estado Emitido"
      );
    }

    logger.info("Verificación post-commit exitosa", { id_recibo });

    // ========================================================================
    // FASE 3: GENERACIÓN DE PDF
    // ========================================================================
  
 
let rutaPdf = null;
let pdfWarning = null;

try {
  // 🔹 Rehidratar recibo con nombre completo del alumno
  const [[reciboParaPdf]] = await pool.execute(
    `
    SELECT
      r.*,
      CONCAT_WS(' ',
        a.apellido_paterno,
        a.apellido_materno,
        a.nombre
      ) AS alumno_nombre_completo
    FROM recibos r
    JOIN alumnos a ON a.id_alumno = r.id_alumno
    WHERE r.id_recibo = ?
      AND r.status_recibo = 'Emitido'
    `,
    [txResult.id_recibo]
  );

  if (!reciboParaPdf) {
    throw new Error("No se pudo obtener recibo emitido para PDF");
  }

  const [detalles] = await pool.execute(
    `SELECT * FROM recibos_detalle WHERE id_recibo = ?`,
    [txResult.id_recibo]
  );

  const pdfBuffer = await generateReciboPDF(reciboParaPdf, detalles);
  const pdfPath = getReciboPdfPath(nombre_recibo);

  await deleteFileIfExists(pdfPath);
  rutaPdf = await uploadPdfToGCS(pdfBuffer, pdfPath);

  logger.info("PDF generado y subido", { id_recibo: txResult.id_recibo, rutaPdf });

} catch (pdfError) {
  logger.error("Error al generar PDF del recibo emitido", {
    id_recibo: txResult.id_recibo,
    error: pdfError.message,
    stack: pdfError.stack
  });

  pdfWarning = "Recibo emitido pero PDF no generado. Puede regenerarse.";

  await pool.execute(
    `
    UPDATE recibos
    SET generando_pdf = FALSE
    WHERE id_recibo = ?
    `,
    [txResult.id_recibo]
  );
}









    // ========================================================================
    // FASE 4: ACTUALIZAR RUTA PDF (solo si se generó)
    // ========================================================================
    if (rutaPdf) {
      const [updateResult] = await pool.execute(
        `
        UPDATE recibos
        SET
          ruta_pdf = ?,
          generando_pdf = FALSE
        WHERE id_recibo = ?
          AND status_recibo = 'Emitido'
          AND generando_pdf = TRUE
        `,
        [rutaPdf, txResult.id_recibo]
      );

      if (updateResult.affectedRows === 0) {
        logger.warn("El recibo cambió de estado durante generación de PDF", {
          id_recibo: txResult.id_recibo
        });
        throw new Error(
          "El recibo cambió de estado durante la generación del PDF"
        );
      }
    }

    // ========================================================================
    // PREPARAR RESPUESTA EXITOSA
    // ========================================================================
    const duration = Date.now() - startTime;
    
    logger.info("Recibo emitido exitosamente", {
      id_recibo: txResult.id_recibo,
      encorte: txResult.corteId,
      pdf_generado: !!rutaPdf,
      duration_ms: duration
    });

    resultado = {
      ok: true,
      id_recibo: txResult.id_recibo,
      encorte: txResult.corteId,
      ruta_pdf: rutaPdf,
      processing_time_ms: duration,
      timestamp: new Date().toISOString(),
      ...(pdfWarning && { warning: pdfWarning })
    };

  } catch (error) {
    // ========================================================================
    // CAPTURAR ERROR PARA MANEJO CENTRALIZADO
    // ========================================================================
    errorFinal = error;
    
    const duration = Date.now() - startTime;
    const errorContext = {
      id_recibo,
      nombre_recibo,
      timestamp: new Date().toISOString(),
      duration_ms: duration,
      error_message: error.message,
      error_stack: error.stack
    };

    // Limpiar lock técnico si existe
    try {
      await pool.execute(
        `
        UPDATE recibos
        SET generando_pdf = FALSE
        WHERE id_recibo = ?
          AND generando_pdf = TRUE
        `,
        [id_recibo]
      );
    } catch (cleanupError) {
      logger.error("Error en limpieza de lock", { 
        id_recibo,
        error: cleanupError.message 
      });
    }

    // Logging estructurado por tipo de error
    if (error.message.includes("duplicado")) {
      logger.warn("Intento de duplicado detectado", errorContext);
    } else if (error.message.includes("no encontrado")) {
      logger.warn("Recibo no disponible", errorContext);
    } else if (error.message.includes("incompletos")) {
      logger.warn("Recibo con datos inválidos", errorContext);
    } else if (error.message.includes("total mayor a cero")) {
      logger.warn("Recibo con total inválido", errorContext);
    } else if (error.message.includes("ERROR CRÍTICO")) {
      logger.error("INCONSISTENCIA DE ESTADO", errorContext);
    } else {
      logger.error("Error en emisión de recibo", errorContext);
    }

  } finally {
    // ========================================================================
    // LIMPIEZA SIEMPRE (independiente de éxito/error)
    // ========================================================================
    try {
      await pool.execute(
        `
        UPDATE recibos
        SET enimpresion = FALSE
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );
    } catch (cleanupError) {
      logger.error("Error limpiando enimpresion", {
        id_recibo,
        error: cleanupError.message
      });
    }
  }


  // ============================================================================
  // PUNTO ÚNICO DE SALIDA
  // ============================================================================
  if (errorFinal) {
    next(errorFinal);
  } else {
    res.json(resultado);
  }
});

 // ============================================================================
  // ENDPOINT: CANCELAR RECIBO
  // ============================================================================

app.post("/cancelar-recibo", requireToken, async (req, res, next) => {
  const startTime = Date.now();
  const { id_recibo } = req.body;

  if (!id_recibo) {
    return res.status(400).json({
      ok: false,
      error: "id_recibo es requerido"
    });
  }

  let reciboSnapshot = null;
  let rutaPdfFinal = null;

  try {
    // ============================================================
    // FASE 1: TRANSACCIÓN (VALIDAR + CANCELAR + RECALCULAR CORTE)
    // ============================================================
    await executeInTransaction(async (conn) => {

      const [[recibo]] = await conn.execute(
        `
        SELECT *
        FROM recibos
        WHERE id_recibo = ?
          AND solicita_cancelacion = TRUE
          AND status_recibo = 'Emitido'
          AND (generando_pdf IS NULL OR generando_pdf = FALSE)
        FOR UPDATE
        `,
        [id_recibo]
      );

      if (!recibo) {
        throw new Error("Recibo no válido para cancelación");
      }

      reciboSnapshot = recibo;

      // Cancelar recibo + activar lock técnico
      await conn.execute(
        `
        UPDATE recibos
        SET
          status_recibo = 'Cancelado',
          fecha_cancelacion = NOW(),
          solicita_cancelacion = FALSE,
          generando_pdf = TRUE
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );

      // Recalcular corte SOLO si el recibo estaba en uno
      if (recibo.encorte) {
        await conn.execute(
          `CALL sp_recalcular_corte(?)`,
          [recibo.encorte]
        );
      }
    });

    // ============================================================
    // FASE 2: REGENERAR PDF (CANCELADO)
    // ============================================================
    const [[reciboParaPdf]] = await pool.execute(
      `
      SELECT
        r.*,
        CONCAT_WS(' ',
          a.apellido_paterno,
          a.apellido_materno,
          a.nombre
        ) AS alumno_nombre_completo
      FROM recibos r
      JOIN alumnos a ON a.id_alumno = r.id_alumno
      WHERE r.id_recibo = ?
        AND r.status_recibo = 'Cancelado'
      `,
      [id_recibo]
    );

    if (!reciboParaPdf) {
      throw new Error("No se pudo obtener recibo cancelado para PDF");
    }

    const [detalles] = await pool.execute(
      `SELECT * FROM recibos_detalle WHERE id_recibo = ?`,
      [id_recibo]
    );

    if (!detalles || detalles.length === 0) {
      throw new Error("Recibo cancelado sin detalles (inconsistencia)");
    }

    const pdfBuffer = await generateReciboPDF(reciboParaPdf, detalles);

    // Resolver ruta PDF (sobrescritura)
    if (reciboParaPdf.ruta_pdf) {
      const prefix = `gs://${config.gcs.bucket}/`;
      rutaPdfFinal = reciboParaPdf.ruta_pdf.startsWith(prefix)
        ? reciboParaPdf.ruta_pdf.replace(prefix, "")
        : `recibos/${id_recibo}.pdf`;
    } else {
      rutaPdfFinal = `recibos/${id_recibo}.pdf`;
    }

    await deleteFileIfExists(rutaPdfFinal);
    const rutaGs = await uploadPdfToGCS(pdfBuffer, rutaPdfFinal);

    // Guardar ruta + liberar lock
    await pool.execute(
      `
      UPDATE recibos
      SET
        ruta_pdf = ?,
        generando_pdf = FALSE
      WHERE id_recibo = ?
      `,
      [rutaGs, id_recibo]
    );

    // ============================================================
    // RESPUESTA EXITOSA
    // ============================================================
    res.json({
      ok: true,
      id_recibo,
      status: "Cancelado",
      ruta_pdf: rutaGs,
      duration_ms: Date.now() - startTime
    });

  } catch (error) {
    next(error);

  } finally {
    // ============================================================
    // LIMPIEZA GARANTIZADA (FLAG + LOCK)
    // ============================================================
    try {
      await pool.execute(
        `
        UPDATE recibos
        SET
          generando_pdf = FALSE,
          solicita_cancelacion = FALSE
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );
    } catch (cleanupError) {
      logger.error("ERROR CRÍTICO: No se pudo limpiar flags de cancelación", {
        id_recibo,
        error: cleanupError.message,
        ACCION_REQUERIDA: "Revisar recibo manualmente en BD"
      });
    }
  }
});


// ============================================================================
// VER PDF GENÉRICO (RECIBOS, CORTES, ETC) - URL FIRMADO
// ============================================================================
app.get("/pdf/:tipo/:id/ver", async (req, res, next) => {
  const { tipo, id } = req.params;
  const token = req.query.token;

  try {
    // ------------------------------------------------------------------------
    // 1. Validación de entrada
    // ------------------------------------------------------------------------
    if (!token || token !== config.apiToken) {
      logger.warn("Intento de acceso sin token válido", { tipo, id });
      return res.status(401).send("Token inválido");
    }

    if (!id || id.trim().length === 0) {
      logger.warn("ID vacío en solicitud de PDF", { tipo, id });
      return res.status(400).send("ID inválido");
    }

    // Validación básica de formato UUID (opcional pero recomendado)
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(id)) {
      logger.warn("Formato de UUID inválido", { tipo, id });
      return res.status(400).send("Formato de ID inválido");
    }

    logger.info("Solicitud de PDF recibida", { tipo, id });

    // ------------------------------------------------------------------------
    // 2. Resolver configuración según tipo de documento
    // ------------------------------------------------------------------------
    const tiposPermitidos = {
      recibo: {
        tabla: "recibos",
        campo_id: "id_recibo",
        query: `
          SELECT ruta_pdf
          FROM recibos
          WHERE id_recibo = ?
            AND status_recibo = 'Emitido'
            AND ruta_pdf IS NOT NULL
        `
      },
      corte: {
        tabla: "cortes",
        campo_id: "id_corte",
        query: `
          SELECT ruta_pdf
          FROM cortes
          WHERE id_corte = ?
            AND ruta_pdf IS NOT NULL
        `
      }
    };

    const config_tipo = tiposPermitidos[tipo];
    
    if (!config_tipo) {
      logger.warn("Tipo de documento no soportado", { tipo, id });
      return res.status(400).send("Tipo de documento no soportado");
    }

    // ------------------------------------------------------------------------
    // 3. Obtener ruta del PDF desde BD
    // ------------------------------------------------------------------------
    const [[row]] = await pool.execute(config_tipo.query, [id]);

    if (!row || !row.ruta_pdf) {
      logger.warn("PDF no encontrado en BD", { 
        tipo, 
        id, 
        tabla: config_tipo.tabla 
      });
      return res.status(404).send("PDF no disponible");
    }

    // ------------------------------------------------------------------------
    // 4. Validar formato de ruta y extraer path
    // ------------------------------------------------------------------------
    const rutaPdf = row.ruta_pdf;
    const bucketPrefix = `gs://${config.gcs.bucket}/`;

    if (!rutaPdf.startsWith(bucketPrefix)) {
      logger.error("Formato de ruta_pdf inválido", { 
        tipo, 
        id, 
        ruta_pdf: rutaPdf 
      });
      return res.status(500).send("Error en formato de archivo");
    }

    const filePath = rutaPdf.replace(bucketPrefix, "");

    // ------------------------------------------------------------------------
    // 5. Generar signed URL con expiración corta
    // ------------------------------------------------------------------------
    const [signedUrl] = await bucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 60 * 1000 // 60 segundos
    });

    logger.info("Signed URL generado exitosamente", {
      tipo,
      id,
      tabla: config_tipo.tabla,
      filePath,
      expira_en_segundos: 60
    });

    // ------------------------------------------------------------------------
    // 6. Redirigir al PDF
    // ------------------------------------------------------------------------
    res.redirect(signedUrl);

  } catch (error) {
    logger.error("Error al generar URL firmado", {
      tipo,
      id,
      error: error.message,
      stack: error.stack
    });
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




  // ------------------------------------------------------------------------
    // X. Regenerar PDF (para reintentos)
    // ------------------------------------------------------------------------

app.post("/recibos/regenerar-pdf", requireToken, async (req, res, next) => {
  const { id_recibo } = req.body;
  const startTime = Date.now();
  const correlationId = `regen-${Date.now()}`;

  // ============================================================
  // VALIDACIÓN Y SANITIZACIÓN
  // ============================================================
  if (!id_recibo) {
    return res.status(400).json({
      ok: false,
      error: "id_recibo es requerido"
    });
  }

  const reciboIdSanitized = String(id_recibo).trim();
  if (!reciboIdSanitized || reciboIdSanitized.length > 255) {
    return res.status(400).json({
      ok: false,
      error: "id_recibo inválido"
    });
  }

  logger.info("Solicitud de regeneración de PDF recibida", {
    correlation_id: correlationId,
    id_recibo: reciboIdSanitized
  });

  let recibo = null;
  let rutaPdfFinal = null;
  let pdfBuffer = null;

  try {
    // ============================================================
// FASE 1: BLOQUEO + VALIDACIÓN (TRANSACCIÓN)
// ============================================================
const lockAcquiredAt = Date.now();

await executeInTransaction(async (conn) => {

  const [[row]] = await conn.execute(
    `
    SELECT *
    FROM recibos
    WHERE id_recibo = ?
      AND status_recibo IN ('Emitido','Cancelado')
      AND (generando_pdf IS NULL OR generando_pdf = FALSE)
    FOR UPDATE
    `,
    [reciboIdSanitized]
  );

  if (!row) {
    throw new Error(
      "Recibo no encontrado, no válido para regeneración o está siendo procesado"
    );
  }

  // Activar lock técnico
  const [lockResult] = await conn.execute(
    `
    UPDATE recibos
    SET generando_pdf = TRUE
    WHERE id_recibo = ?
    `,
    [reciboIdSanitized]
  );

  if (lockResult.affectedRows !== 1) {
    throw new Error("No se pudo activar lock de generación de PDF");
  }

  // Verificar estado post-lock
  const [[verificacion]] = await conn.execute(
    `
    SELECT generando_pdf
    FROM recibos
    WHERE id_recibo = ?
    `,
    [reciboIdSanitized]
  );

  if (!verificacion || verificacion.generando_pdf !== 1) {
    throw new Error(
      "Lock no se activó correctamente en BD"
    );
  }

  recibo = row; // Ya no se usa para PDF, solo para rutaPdf

  const lockDuration = Date.now() - lockAcquiredAt;

  logger.info("Lock técnico activado para regeneración de PDF", {
    correlation_id: correlationId,
    id_recibo: reciboIdSanitized,
    status_recibo: recibo.status_recibo,
    lock_duration_ms: lockDuration
  });
});

    // ============================================================
    // FASE 2: RESOLVER RUTA DEL PDF
    // ============================================================
    if (recibo.ruta_pdf) {
      const bucketPrefix = `gs://${config.gcs.bucket}/`;
      
      // Validar que ruta_pdf coincida con bucket configurado
      if (!recibo.ruta_pdf.startsWith(bucketPrefix)) {
        throw new Error(
          "ruta_pdf en BD no coincide con bucket configurado"
        );
      }
      
      rutaPdfFinal = recibo.ruta_pdf.replace(bucketPrefix, "");
      
      // Validar que no tenga path traversal
      if (rutaPdfFinal.includes('..') || rutaPdfFinal.startsWith('/')) {
        throw new Error(
          "ruta_pdf contiene caracteres peligrosos"
        );
      }

      logger.info("Usando ruta_pdf existente para sobrescritura", {
        correlation_id: correlationId,
        id_recibo: reciboIdSanitized,
        ruta_pdf: recibo.ruta_pdf
      });
    } else {
      // FALLBACK DETERMINÍSTICO: Si el recibo nunca tuvo PDF o se perdió
      // la referencia, usamos id_recibo para garantizar unicidad.
      // Esto puede ocurrir si:
      // - El recibo se emitió antes de guardar ruta_pdf
      // - Hubo un error parcial en emisión original
      // - Migración de datos históricos
      rutaPdfFinal = `recibos/${reciboIdSanitized}.pdf`;

      logger.warn("Recibo sin ruta_pdf, usando fallback por id_recibo", {
        correlation_id: correlationId,
        id_recibo: reciboIdSanitized,
        ruta_generada: rutaPdfFinal
      });
    }

  // ============================================================
// FASE 3: GENERAR PDF
// ============================================================

// 🔹 Rehidratar recibo con nombre completo del alumno
const [[reciboParaPdf]] = await pool.execute(
  `
  SELECT
    r.*,
    CONCAT_WS(' ',
      a.apellido_paterno,
      a.apellido_materno,
      a.nombre
    ) AS alumno_nombre_completo
  FROM recibos r
  JOIN alumnos a ON a.id_alumno = r.id_alumno
  WHERE r.id_recibo = ?
    AND r.status_recibo IN ('Emitido', 'Cancelado')
  `,
  [reciboIdSanitized]
);

if (!reciboParaPdf) {
  throw new Error(
    "No se pudo obtener recibo con datos de alumno para PDF"
  );
}

// Validar datos mínimos necesarios para PDF
if (!reciboParaPdf.id_alumno || !reciboParaPdf.fecha_emision) {
  throw new Error(
    "Recibo con datos incompletos - no se puede generar PDF"
  );
}

const [detalles] = await pool.execute(
  `SELECT * FROM recibos_detalle WHERE id_recibo = ?`,
  [reciboIdSanitized]
);

// Validar que existan detalles
if (!detalles || detalles.length === 0) {
  throw new Error(
    "Recibo sin detalles - no se puede generar PDF"
  );
}

logger.info("Iniciando generación de PDF", {
  correlation_id: correlationId,
  id_recibo: reciboIdSanitized,
  num_detalles: detalles.length,
  status_recibo: reciboParaPdf.status_recibo,
  alumno: reciboParaPdf.alumno_nombre_completo
});

pdfBuffer = await generateReciboPDF(reciboParaPdf, detalles);

// Validar que el PDF se generó correctamente
if (!pdfBuffer || pdfBuffer.length === 0) {
  throw new Error(
    "PDF generado está vacío - operación abortada"
  );
}

logger.info("PDF generado en memoria", {
  correlation_id: correlationId,
  id_recibo: reciboIdSanitized,
  buffer_size_kb: (pdfBuffer.length / 1024).toFixed(2)
});

    // ============================================================
    // FASE 4: SOBRESCRIBIR ARCHIVO EN GCS
    // ============================================================
    logger.info("Iniciando subida a GCS", {
      correlation_id: correlationId,
      id_recibo: reciboIdSanitized,
      ruta_destino: rutaPdfFinal,
      buffer_size_kb: (pdfBuffer.length / 1024).toFixed(2)
    });

    await deleteFileIfExists(rutaPdfFinal);
    const rutaGs = await uploadPdfToGCS(pdfBuffer, rutaPdfFinal);

    // Validar que GCS retornó ruta válida
    if (!rutaGs || !rutaGs.startsWith('gs://')) {
      throw new Error(
        "Ruta GCS inválida devuelta por uploadPdfToGCS"
      );
    }

    logger.info("PDF regenerado y subido correctamente", {
      correlation_id: correlationId,
      id_recibo: reciboIdSanitized,
      ruta_gs: rutaGs
    });

    // ============================================================
    // FASE 5: ACTUALIZAR BD (si era fallback)
    // ============================================================
    const esRegeneracionConFallback = !recibo.ruta_pdf;

    if (esRegeneracionConFallback) {
      await pool.execute(
        `
        UPDATE recibos
        SET ruta_pdf = ?
        WHERE id_recibo = ?
        `,
        [rutaGs, reciboIdSanitized]
      );

      logger.info("ruta_pdf actualizada por fallback", {
        correlation_id: correlationId,
        id_recibo: reciboIdSanitized,
        ruta_pdf: rutaGs
      });
    }

    const duration = Date.now() - startTime;

    logger.info("Regeneración de PDF completada exitosamente", {
      correlation_id: correlationId,
      id_recibo: reciboIdSanitized,
      ruta_pdf: rutaGs,
      duration_ms: duration,
      pdf_size_kb: (pdfBuffer.length / 1024).toFixed(2),
      status_recibo: recibo.status_recibo,
      fallback_usado: esRegeneracionConFallback
    });

    res.json({
      ok: true,
      id_recibo: reciboIdSanitized,
      ruta_pdf: rutaGs,
      regenerated: true,
      duration_ms: duration
    });

  } catch (error) {
    logger.error("Error al regenerar PDF del recibo", {
      correlation_id: correlationId,
      id_recibo: reciboIdSanitized,
      error_message: error.message,
      stack: error.stack,
      duration_ms: Date.now() - startTime
    });

    next(error);

  } finally {
    // ============================================================
    // FASE 6: LIMPIEZA GARANTIZADA DEL LOCK
    // ============================================================
    try {
      await pool.execute(
        `
        UPDATE recibos
        SET generando_pdf = FALSE
        WHERE id_recibo = ?
        `,
        [reciboIdSanitized]
      );

      logger.info("Lock técnico liberado (generando_pdf = FALSE)", {
        correlation_id: correlationId,
        id_recibo: reciboIdSanitized
      });

    } catch (cleanupError) {
      logger.error("ERROR CRÍTICO: No se pudo limpiar generando_pdf", {
        correlation_id: correlationId,
        id_recibo: reciboIdSanitized,
        error: cleanupError.message,
        ACCION_REQUERIDA: "Revisar manualmente registro en BD y limpiar flag"
      });
    }
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
