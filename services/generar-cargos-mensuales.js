module.exports = function generarCargosMensualesFactory({
  pool,
  executeInTransaction,
  logger
}) {
  return async function generarCargosMensualesHandler(req, res, next) {
    const startTime = Date.now();
    let { mes, anio, alumnos } = req.body;

    logger.info("Inicio de generarCargosMensualesHandler", {
      mes_recibido: mes,
      anio_recibido: anio,
      alumnos_recibidos: alumnos,
      body: req.body
    });

    try {

      logger.info("Iniciando transacción para generación de cargos");

      const result = await executeInTransaction(async (conn) => {

        const txStart = Date.now();

        // ============================================================
        // 1️⃣ Resolver mes y año (default = MySQL Mexico timezone)
        // ============================================================

        if (mes == null || anio == null) {
          logger.info("Mes o año no proporcionados, obteniendo desde MySQL (CURDATE Mexico)");

          const [[rowFecha]] = await conn.execute(`
            SELECT 
              MONTH(CURDATE()) AS mes,
              YEAR(CURDATE()) AS anio
          `);

          mes = rowFecha.mes;
          anio = rowFecha.anio;

          logger.info("Valores por defecto obtenidos desde MySQL", {
            mes_calculado: mes,
            anio_calculado: anio
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
          throw new Error("Mes inválido");
        }

        if (anio < 2000 || anio > 2100) {
          logger.warn("Validación fallida: año inválido", { anio });
          throw new Error("Año inválido");
        }

        logger.info("Preparando generación de cargos mensuales", { mes, anio });

        const alumnosList = alumnos
          ? String(alumnos).split(",").map(a => a.trim())
          : null;

        logger.info("Lista de alumnos procesada", {
          alumnosList,
          cantidad_alumnos: alumnosList ? alumnosList.length : 'todos'
        });

        logger.info("Ejecutando INSERT SELECT en alumnos_cargos", {
          mes,
          anio,
          filtro_alumnos: alumnosList ? 'aplicado' : 'todos'
        });

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
          JOIN alumnos a 
            ON a.id_alumno = am.id_alumno
          JOIN productos p 
            ON p.id_producto = am.id_producto
          JOIN planteles pl
            ON pl.id_plantel = a.id_plantel_academico
          WHERE 
            a.status = 'Activo'
            AND p.status = 'Activo'
            AND p.frecuencia = 'Mensual'
            AND pl.status = 'Activo'
          ${alumnosList && alumnosList.length > 0 
            ? `AND am.id_alumno IN (${alumnosList.map(() => '?').join(',')})`
            : ''
          }
          ON DUPLICATE KEY UPDATE
            status_cargo = 'Activo',
            motivo_cancelacion = NULL,
            updated_at = NOW()
          `,
          [
            mes,
            anio,
            ...(alumnosList && alumnosList.length > 0 ? alumnosList : [])
          ]
        );

        const txDuration = Date.now() - txStart;

        logger.info("INSERT SELECT ejecutado en alumnos_cargos", {
          affectedRows: executeResult.affectedRows,
          insertId: executeResult.insertId,
          warningStatus: executeResult.warningStatus,
          mes,
          anio,
          filtro_alumnos: alumnosList ? 'aplicado' : 'todos',
          tx_duration_ms: txDuration
        });

        logger.info("Transacción interna completada correctamente", {
          mes,
          anio,
          total_procesados: executeResult.affectedRows,
          tx_duration_ms: txDuration
        });

        return {
          mes,
          anio,
          procesados: executeResult.affectedRows,
          tx_duration_ms: txDuration
        };
      });

      const duration = Date.now() - startTime;

      logger.info("Cargos mensuales generados correctamente", {
        mes: result.mes,
        anio: result.anio,
        procesados: result.procesados,
        tx_duration_ms: result.tx_duration_ms,
        total_duration_ms: duration
      });

      res.json({
        ok: true,
        mes: result.mes,
        anio: result.anio,
        procesados: result.procesados,
        tx_duration_ms: result.tx_duration_ms,
        total_duration_ms: duration
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
        alumnos,
        duration_ms: Date.now() - startTime
      });

      next(error);
    }
  };
};