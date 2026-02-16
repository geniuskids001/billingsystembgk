module.exports = function sincronizarProductosAlumnoFactory({
  pool,
  executeInTransaction,
  logger
}) {

  return async function sincronizarProductosAlumnoHandler(req, res, next) {

    const { id_alumno } = req.body;
    const startTime = Date.now();

    try {
      const result = await executeInTransaction(async (conn) => {

        // ============================================================
        // 1️⃣ Obtener datos actuales del alumno (con lock pesimista)
        // ============================================================
        console.log(`[SincronizarProductos] Consultando datos del alumno ${id_alumno} con lock`);
        
        const [[alumno]] = await conn.execute(
          `
          SELECT 
            a.id_alumno,
            a.id_plantel_academico,
            a.id_grupo,
            g.nivel
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
        const nivelActual = alumno.nivel || null;

        console.log(`[SincronizarProductos] Datos del alumno - Plantel: ${plantelActual}, Nivel: ${nivelActual || 'sin grupo'}`);

        // ============================================================
        // 2️⃣ ELIMINAR asignaciones inválidas
        // ============================================================
        console.log(`[SincronizarProductos] Eliminando productos inválidos para alumno ${id_alumno}`);
        
        const [deleteResult] = await conn.execute(
          `
          DELETE am
          FROM alumnos_mensuales am
          JOIN productos p ON p.id_producto = am.id_producto
          WHERE am.id_alumno = ?
            AND (
                  p.id_plantel != ?
                  OR (
                       p.aplica_nivel IS NOT NULL
                       AND (
                             ? IS NULL
                             OR p.aplica_nivel != ?
                           )
                     )
                )
          `,
          [id_alumno, plantelActual, nivelActual, nivelActual]
        );

        console.log(`[SincronizarProductos] Productos eliminados: ${deleteResult.affectedRows}`);

        // ============================================================
        // 3️⃣ INSERTAR productos default globales
        // ============================================================
        console.log(`[SincronizarProductos] Insertando productos default globales para alumno ${id_alumno}`);
        
        const [insertGlobalesResult] = await conn.execute(
          `
          INSERT INTO alumnos_mensuales (
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
          ON DUPLICATE KEY UPDATE id_producto = id_producto
          `,
          [id_alumno, plantelActual]
        );

        console.log(`[SincronizarProductos] Productos globales insertados/actualizados: ${insertGlobalesResult.affectedRows}`);

        // ============================================================
        // 4️⃣ INSERTAR productos default por nivel
        // ============================================================
        if (nivelActual) {
          console.log(`[SincronizarProductos] Insertando productos default para nivel "${nivelActual}"`);
          
          const [insertNivelResult] = await conn.execute(
            `
            INSERT INTO alumnos_mensuales (
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
            ON DUPLICATE KEY UPDATE id_producto = id_producto
            `,
            [id_alumno, plantelActual, nivelActual]
          );
          
          console.log(`[SincronizarProductos] Productos por nivel insertados/actualizados: ${insertNivelResult.affectedRows}`);
        } else {
          console.log(`[SincronizarProductos] Alumno ${id_alumno} sin grupo/nivel: se omiten productos específicos por nivel`);
        }

        console.log(`[SincronizarProductos] Sincronización completada exitosamente para alumno ${id_alumno}`);

        return { ok: true };
      });

      const durationMs = Date.now() - startTime;
      console.log(`[SincronizarProductos] Operación completada en ${durationMs}ms para alumno ${id_alumno}`);

      res.json({
        ok: true,
        id_alumno,
        duration_ms: durationMs
      });

    } catch (error) {
      console.error(`[SincronizarProductos] Error en sincronización para alumno ${id_alumno}:`, {
        message: error.message,
        stack: error.stack,
        duration_ms: Date.now() - startTime
      });
      next(error);
    }

  };

};
