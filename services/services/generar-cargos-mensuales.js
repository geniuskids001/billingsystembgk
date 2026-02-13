module.exports = function generarCargosMensualesFactory({
  pool,
  executeInTransaction,
  logger
}) {
  return async function generarCargosMensualesHandler(req, res, next) {
    const startTime = Date.now();
    let { mes, anio } = req.body;
    
    logger.info("Inicio de generarCargosMensualesHandler", {
      mes_recibido: mes,
      anio_recibido: anio,
      body: req.body
    });
    
    try {
      // ============================================================
      // 1️⃣ Resolver mes y año (default = Mexico time actual)
      // ============================================================
      if (!mes || !anio) {
        logger.info("Mes o año no proporcionados, calculando valores por defecto");
        
        const now = new Date(
          new Date().toLocaleString("en-US", { timeZone: "America/Mexico_City" })
        );
        mes = now.getMonth() + 1;
        anio = now.getFullYear();
        
        logger.info("Valores por defecto calculados", {
          mes_calculado: mes,
          anio_calculado: anio,
          fecha_mexico: now.toISOString()
        });
      }
      
      mes = Number(mes);
      anio = Number(anio);
      
      logger.info("Valores de mes y año parseados", {
        mes_final: mes,
        anio_final: anio
      });
      
      if (mes < 1 || mes > 12) {
        logger.warn("Validación fallida: mes inválido", { mes });
        return res.status(400).json({ ok: false, error: "Mes inválido" });
      }
      if (anio < 2000 || anio > 2100) {
        logger.warn("Validación fallida: año inválido", { anio });
        return res.status(400).json({ ok: false, error: "Año inválido" });
      }
      
      logger.info("Generando cargos mensuales", { mes, anio });
      
      // ============================================================
      // 2️⃣ Transacción completa
      // ============================================================
      logger.info("Iniciando transacción para generación de cargos");
      
      const result = await executeInTransaction(async (conn) => {
        logger.info("Transacción iniciada, ejecutando INSERT SELECT para alumnos_cargos");
        
        const [executeResult] = await conn.execute(
          `
          INSERT INTO alumnos_cargos (
            id_cargo,
            id_alumno,
            id_producto,
            mes,
            anio,
            status_cargo
          )
          SELECT 
            UUID(),
            am.id_alumno,
            am.id_producto,
            ?,
            ?,
            'Activo'
          FROM alumnos_mensuales am
          JOIN alumnos a ON a.id_alumno = am.id_alumno
          WHERE a.status = 'Activo'
          ON DUPLICATE KEY UPDATE
            status_cargo = 'Activo',
            motivo_cancelacion = NULL,
            updated_at = NOW()
          `,
          [mes, anio]
        );
        
        logger.info("INSERT SELECT ejecutado en alumnos_cargos", {
          affectedRows: executeResult.affectedRows,
          insertId: executeResult.insertId,
          warningStatus: executeResult.warningStatus,
          mes,
          anio
        });
        
        const procesados = executeResult.affectedRows;
        
        logger.info("Todos los cargos procesados en transacción", {
          total_procesados: procesados
        });
        
        return { procesados };
      });
      
      logger.info("Transacción completada exitosamente");
      
      const duration = Date.now() - startTime;
      
      logger.info("Cargos mensuales generados correctamente", {
        mes,
        anio,
        procesados: result.procesados,
        duration_ms: duration
      });
      
      res.json({
        ok: true,
        mes,
        anio,
        procesados: result.procesados,
        duration_ms: duration
      });
    } catch (error) {
      logger.error("Error al generar cargos mensuales", {
        error_message: error.message,
        error_code: error.code,
        error_errno: error.errno,
        error_sql: error.sql,
        stack: error.stack,
        mes,
        anio,
        duration_ms: Date.now() - startTime
      });
      
      next(error);
    }
  };
};