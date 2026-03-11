async function generarCargosMensualesCore(conn, mes, anio, alumnosList = null, mode = "soft") {

  // fallback seguridad
  if (!mode) mode = "soft";

  if (mes == null || anio == null) {
    const [[rowFecha]] = await conn.execute(`
      SELECT 
        MONTH(CURDATE()) AS mes,
        YEAR(CURDATE()) AS anio
    `);

    mes = rowFecha.mes;
    anio = rowFecha.anio;
  }

  mes = Number(mes);
  anio = Number(anio);

  if (mes < 1 || mes > 12) {
    throw new Error("Mes inválido");
  }

  if (anio < 2000 || anio > 2100) {
    throw new Error("Año inválido");
  }

  // ============================================
  // SQL dinámico según modo
  // ============================================

  const insertClause = mode === "hard"
    ? "INSERT INTO alumnos_cargos"
    : "INSERT IGNORE INTO alumnos_cargos";

  const duplicateClause = mode === "hard"
    ? `
      ON DUPLICATE KEY UPDATE
        status_cargo = 'Activo',
        motivo_cancelacion = NULL,
        updated_at = NOW()
      `
    : "";

  const [executeResult] = await conn.execute(
    `
    ${insertClause} (
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
    JOIN productos p ON p.id_producto = am.id_producto
    JOIN planteles pl ON pl.id_plantel = a.id_plantel_academico
    WHERE 
      a.status = 'Activo'
      AND p.status = 'Activo'
      AND p.frecuencia = 'Mensual'
      AND pl.status = 'Activo'
      ${alumnosList && alumnosList.length > 0 
        ? `AND am.id_alumno IN (${alumnosList.map(() => '?').join(',')})`
        : ''
      }
    ${duplicateClause}
    `,
    [
      mes,
      anio,
      ...(alumnosList && alumnosList.length > 0 ? alumnosList : [])
    ]
  );

  return {
    mes,
    anio,
    mode,
    procesados: executeResult.affectedRows
  };
}

function generarCargosMensualesFactory({
  pool,
  executeInTransaction,
  logger
}) {
  return async function generarCargosMensualesHandler(req, res, next) {

    const startTime = Date.now();

    let { mes, anio, alumnos, mode } = req.body;

    logger.info("Inicio de generarCargosMensualesHandler", {
      mes_recibido: mes,
      anio_recibido: anio,
      alumnos_recibidos: alumnos,
      mode_recibido: mode,
      body: req.body
    });

    try {

      logger.info("Iniciando transacción para generación de cargos");

      const result = await executeInTransaction(async (conn) => {

        const txStart = Date.now();

        const alumnosList = alumnos
          ? String(alumnos).split(",").map(a => a.trim())
          : null;

        logger.info("Lista de alumnos procesada", {
          alumnosList,
          cantidad_alumnos: alumnosList ? alumnosList.length : 'todos'
        });

        logger.info("Ejecutando generarCargosMensualesCore", {
          mes,
          anio,
          mode,
          filtro_alumnos: alumnosList ? 'aplicado' : 'todos'
        });

        const coreResult = await generarCargosMensualesCore(
          conn,
          mes,
          anio,
          alumnosList,
          mode
        );

        const txDuration = Date.now() - txStart;

        logger.info("Transacción interna completada correctamente", {
          mes: coreResult.mes,
          anio: coreResult.anio,
          mode: coreResult.mode,
          total_procesados: coreResult.procesados,
          tx_duration_ms: txDuration
        });

        return {
          mes: coreResult.mes,
          anio: coreResult.anio,
          mode: coreResult.mode,
          procesados: coreResult.procesados,
          tx_duration_ms: txDuration
        };
      });

      const duration = Date.now() - startTime;

      logger.info("Cargos mensuales generados correctamente", {
        mes: result.mes,
        anio: result.anio,
        mode: result.mode,
        procesados: result.procesados,
        tx_duration_ms: result.tx_duration_ms,
        total_duration_ms: duration
      });

      res.json({
        ok: true,
        mes: result.mes,
        anio: result.anio,
        mode: result.mode,
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
        mode,
        duration_ms: Date.now() - startTime
      });

      next(error);
    }
  };
}

module.exports = generarCargosMensualesFactory;
module.exports.core = generarCargosMensualesCore;