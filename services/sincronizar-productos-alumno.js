const { core: generarCargosMensualesCore } = require("./generar-cargos-mensuales");

async function sincronizarProductosAlumnoCore(conn, id_alumno, logger) {

  // ============================================================
  // 1 Obtener datos actuales del alumno (con lock pesimista)
  // ============================================================
  console.log(`[SincronizarProductos] Consultando datos del alumno ${id_alumno} con lock`);

  const [[alumno]] = await conn.execute(
    `
    SELECT
    a.id_alumno,
    a.id_plantel_academico,
    a.id_grupo,
    g.id_nivel
    FROM alumnos a
    LEFT JOIN grupos g ON g.id_grupo = a.id_grupo
    WHERE a.id_alumno = ?
    FOR UPDATE
    `,
    [id_alumno]
  );

  if (!alumno) {
    console.error(`[SincronizarProductos] Alumno no encontrado: ${id_alumno}`);
    throw new Error("Alumno no encontrado");
  }

  const plantelActual = alumno.id_plantel_academico;
  const nivelActual = alumno.id_nivel || null;

  console.log(`[SincronizarProductos] Datos del alumno - Plantel: ${plantelActual}, Nivel: ${nivelActual || 'sin grupo'}`);

  
  // ============================================================
  // 3 INSERTAR productos default globales
  // ============================================================
  console.log(`[SincronizarProductos] Insertando productos default globales para alumno ${id_alumno}`);

  const [insertGlobalesResult] = await conn.execute(
    `
    INSERT IGNORE INTO alumnos_mensuales (
      id_alumno_mensual,
      id_alumno,
      id_producto
    )
    SELECT
      UUID(),
      ?,
      p.id_producto
    FROM productos p
    WHERE
      p.id_plantel = ?
      AND p.producto_default = TRUE
      AND p.status = 'Activo'
      AND p.frecuencia = 'Mensual'
      AND p.aplica_nivel IS NULL
    `,
    [id_alumno, plantelActual]
  );

  console.log(`[SincronizarProductos] Productos globales insertados/actualizados: ${insertGlobalesResult.affectedRows}`);

  // ============================================================
  // 4 INSERTAR productos default por nivel
  // ============================================================
  if (nivelActual) {
    console.log(`[SincronizarProductos] Insertando productos default para nivel "${nivelActual}"`);

    const [insertNivelResult] = await conn.execute(
      `
      INSERT IGNORE INTO alumnos_mensuales (
        id_alumno_mensual,
        id_alumno,
        id_producto
      )
      SELECT
        UUID(),
        ?,
        p.id_producto
      FROM productos p
      WHERE
        p.id_plantel = ?
        AND p.producto_default = TRUE
        AND p.status = 'Activo'
        AND p.frecuencia = 'Mensual'
        AND p.aplica_nivel = ?
      `,
      [id_alumno, plantelActual, nivelActual]
    );

    console.log(`[SincronizarProductos] Productos por nivel insertados/actualizados: ${insertNivelResult.affectedRows}`);
  } else {
    console.log(`[SincronizarProductos] Alumno ${id_alumno} sin grupo/nivel: se omiten productos especificos por nivel`);
  }

  // ============================================================
  // 5 Generar cargos mensuales usando core compartido
  // ============================================================
  console.log(`[SincronizarProductos] Generando cargos mensuales (core) para alumno ${id_alumno}`);

  const coreResult = await generarCargosMensualesCore(
    conn,
    null,       // mes -> que el core lo resuelva
    null,       // anio -> que el core lo resuelva
    [id_alumno] // solo este alumno
  );

  console.log(`[SincronizarProductos] Cargos generados/reactivados (core): ${coreResult.procesados}`);

  console.log(`[SincronizarProductos] Sincronizacion completada exitosamente para alumno ${id_alumno}`);

  return {
    ok: true,
    cargos_generados: coreResult.procesados
  };
}

function sincronizarProductosAlumnoFactory({
  pool,
  executeInTransaction,
  logger
}) {

  return async function sincronizarProductosAlumnoHandler(req, res, next) {

    const { id_alumno } = req.body;
    const startTime = Date.now();

    try {

      const result = await executeInTransaction(async (conn) => {
        return await sincronizarProductosAlumnoCore(conn, id_alumno, logger);
      });

      const durationMs = Date.now() - startTime;

      logger.info("Sync completado", {
        id_alumno,
        cargos_generados: result.cargos_generados
      });

      res.json({
        ok: true,
        id_alumno,
        duration_ms: durationMs
      });

    } catch (error) {

      console.error(`[SincronizarProductos] Error en sincronizacion para alumno ${id_alumno}:`, {
        message: error.message,
        stack: error.stack,
        duration_ms: Date.now() - startTime
      });

      next(error);

    } finally {

      // ============================================================
      // APAGAR FLAG SIEMPRE (exito o error)
      // ============================================================
      if (id_alumno) {
        try {
          await pool.execute(
            `
            UPDATE alumnos
            SET sync_productos = 0
            WHERE id_alumno = ?
            `,
            [id_alumno]
          );
          console.log(`[SincronizarProductos] Flag sync_productos apagado para alumno ${id_alumno}`);
        } catch (flagError) {
          console.error(`[SincronizarProductos] ERROR apagando flag sync_productos`, {
            id_alumno,
            message: flagError.message
          });
        }
      }

    }

  };

}

module.exports = sincronizarProductosAlumnoFactory;
module.exports.core = sincronizarProductosAlumnoCore;