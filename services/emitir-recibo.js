
module.exports = function emitirReciboFactory({
  pool,
  executeInTransaction,
  logger,
  generateReciboPDF,
  getReciboHydrated,
  generateCorteId,
  getReciboPdfPath,
  deleteFileIfExists,
  uploadPdfToGCS,
calculateReciboTotal
}) {

  return async function emitirReciboHandler(req, res, next) {

    const { id_recibo, nombre_recibo } = req.body;
    const startTime = Date.now();

    // 👉 Aquí pegas TODO tu código del endpoint
    // desde: logger.info("Iniciando emisión de recibo"...)
    // hasta el final
 logger.info("Iniciando emisión de recibo", { id_recibo, nombre_recibo });

  // ============================================================================
  // VARIABLES DE RESULTADO (un solo punto de salida)
  // ============================================================================
  let resultado = null;
  let errorFinal = null;

  try {
    // ========================================================================
    // FASE 1: TRANSACCIÓN COMPLETA Y ROBUSTA
    // ========================================================================
    const txResult = await executeInTransaction(async (conn) => {
      
      // ────────────────────────────────────────────────────────────────────
      // 1.1 Adquirir lock exclusivo con validación de estado y flags
      // ────────────────────────────────────────────────────────────────────
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
        const err = new Error("Recibo no encontrado, no está en Borrador o está siendo procesado");
        err.statusCode = 409;
        throw err;
      }

      logger.info("Lock adquirido en recibo para emisión", { 
        id_recibo,
        status: row.status_recibo,
        generando_pdf: row.generando_pdf,
        total_actual: row.total_recibo
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.2 Validaciones de datos básicos obligatorios
      // ────────────────────────────────────────────────────────────────────
      if (!row.id_alumno || !row.id_plantel || !row.fecha) {
        const err = new Error("Recibo con datos incompletos (alumno, plantel o fecha faltante)");
        err.statusCode = 400;
        throw err;
      }

      if (Number(row.total_recibo) < 0) {
        const err = new Error("El recibo debe tener un total igual o mayor a cero");
        err.statusCode = 400;
        throw err;
      }

      logger.info("Validaciones de datos básicos completadas", { id_recibo });

      // ────────────────────────────────────────────────────────────────────
      // 1.3 Validar existencia de detalles en Borrador
      // ────────────────────────────────────────────────────────────────────
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
        const err = new Error("El recibo no tiene detalles válidos para emitir");
        err.statusCode = 400;
        throw err;
      }

      logger.info("Detalles validados", { 
        id_recibo, 
        cantidad_detalles: detallesCount.total 
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.4 Validar duplicados de productos mensuales
      // ────────────────────────────────────────────────────────────────────
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
        const err = new Error("Ya existe un recibo emitido para el mismo producto, mes y año");
        err.statusCode = 409;
        throw err;
      }

      logger.info("Validación de duplicados completada", { 
        id_recibo,
        id_alumno: row.id_alumno
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.5 Recalcular totales DENTRO de la transacción (SIN for update)
      // ────────────────────────────────────────────────────────────────────
      logger.info("Iniciando recálculo de totales", { id_recibo });
      
      await calculateReciboTotal(conn, id_recibo);

      logger.info("Recálculo de totales completado", { id_recibo });

      // ────────────────────────────────────────────────────────────────────
      // 1.6 Validar total después del recálculo
      // ────────────────────────────────────────────────────────────────────
      const [[rowAfterCalc]] = await conn.execute(
        `
        SELECT total_recibo
        FROM recibos
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );

      if (Number(rowAfterCalc.total_recibo) < 0) {
        const err = new Error("Total inválido después del cálculo");
        err.statusCode = 500;
        throw err;
      }

      logger.info("Total validado después de recálculo", { 
        id_recibo, 
        total_final: rowAfterCalc.total_recibo 
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.7 Generar ID de corte
      // ────────────────────────────────────────────────────────────────────
      const corteId = generateCorteId(row);

      logger.info("ID de corte generado", { 
        id_recibo, 
        corteId 
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.8 Actualizar recibo a Emitido con validación de estado
      // ────────────────────────────────────────────────────────────────────
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
          AND status_recibo = 'Borrador'
        `,
        [corteId, id_recibo]
      );

      if (updateRecibo.affectedRows !== 1) {
        logger.error("ERROR CRÍTICO: UPDATE recibos no afectó la fila esperada", {
          id_recibo,
          corteId,
          affectedRows: updateRecibo.affectedRows,
          expectedRows: 1
        });

        const err = new Error(
          `ERROR CRÍTICO: UPDATE recibos no afectó filas (affectedRows=${updateRecibo.affectedRows}). Posible cambio de estado concurrente.`
        );
        err.statusCode = 500;
        throw err;
      }

      logger.info("Recibo actualizado a Emitido exitosamente", {
        id_recibo,
        corteId,
        affectedRows: updateRecibo.affectedRows
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.9 Verificación post-UPDATE (debug/auditoría)
      // ────────────────────────────────────────────────────────────────────
      const [debugRows] = await conn.execute(
        `
        SELECT
          status_recibo,
          fecha_emision,
          encorte,
          generando_pdf,
          total_recibo
        FROM recibos
        WHERE id_recibo = ?
        `,
        [id_recibo]
      );

      logger.info("DEBUG: Estado del recibo post-UPDATE (dentro de TX)", {
        id_recibo,
        estado_actual: debugRows[0]
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.10 Actualizar detalles a Emitido
      // ────────────────────────────────────────────────────────────────────
      const [updateDetalles] = await conn.execute(
        `
        UPDATE recibos_detalle
        SET status_detalle = 'Emitido'
        WHERE id_recibo = ?
          AND status_detalle = 'Borrador'
        `,
        [id_recibo]
      );

      logger.info("Detalles actualizados a Emitido", { 
        id_recibo,
        detalles_actualizados: updateDetalles.affectedRows
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.11 Recalcular corte mediante stored procedure
      // ────────────────────────────────────────────────────────────────────
      logger.info("Ejecutando sp_recalcular_corte", { 
        id_recibo, 
        corteId 
      });

      await conn.execute(
        `CALL sp_recalcular_corte(?)`,
        [corteId]
      );

      logger.info("sp_recalcular_corte ejecutado exitosamente", { 
        id_recibo, 
        corteId 
      });

      // ────────────────────────────────────────────────────────────────────
      // 1.12 Transacción completada - retornar datos
      // ────────────────────────────────────────────────────────────────────
      logger.info("Transacción de emisión completada exitosamente", { 
        id_recibo, 
        corteId,
        total_final: rowAfterCalc.total_recibo
      });

      return {
        id_recibo,
        corteId,
        id_alumno: row.id_alumno,
        id_plantel: row.id_plantel,
        total: rowAfterCalc.total_recibo
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

  // 🔹 Obtener recibo hidratado usando helper
  const reciboParaPdf = await getReciboHydrated(txResult.id_recibo);


  if (!reciboParaPdf) {
    throw new Error("No se pudo obtener recibo emitido para PDF");
  }

if (reciboParaPdf.status_recibo !== 'Emitido') {
  throw new Error("El recibo no está en estado Emitido");
}

  const [detalles] = await pool.execute(
    `SELECT * FROM recibos_detalle WHERE id_recibo = ?`,
    [txResult.id_recibo]
  );

console.log("DEBUG BEFORE generateReciboPDF", {
  id_recibo: reciboParaPdf?.id_recibo,
  status: reciboParaPdf?.status_recibo,
  alumno: reciboParaPdf?.alumno_nombre_completo,
  total: reciboParaPdf?.total_recibo,
  detalles_count: detalles?.length,
  generateReciboPDF_type: typeof generateReciboPDF
});



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

  console.log("DEBUG UPDATE RUTA_PDF EMITIR", {
    id_recibo: txResult.id_recibo,
    ruta_pdf: rutaPdf,
    affectedRows: updateResult.affectedRows
  });

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
}
};
