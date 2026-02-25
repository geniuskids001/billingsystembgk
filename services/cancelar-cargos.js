module.exports = function cancelarCargosFactory({
  pool,
  executeInTransaction,
  logger
}) {
  return async function cancelarCargosHandler(req, res, next) {

    let resultado = { procesados: 0, cancelados: 0 };

    try {

      resultado = await executeInTransaction(async (conn) => {

        // 1️⃣ Lock cargos con flag
        const [cargos] = await conn.execute(
          `
          SELECT *
          FROM alumnos_cargos
          WHERE solicita_cancelacion = 1
          FOR UPDATE
          `
        );

        if (cargos.length === 0) {
          return { procesados: 0, cancelados: 0 };
        }

        let cancelados = 0;

        for (const cargo of cargos) {

          // 2️⃣ Validar que NO exista recibo emitido
          const [rows] = await conn.execute(
            `
            SELECT 1
            FROM recibos r
            JOIN recibos_detalle rd 
              ON rd.id_recibo = r.id_recibo
            WHERE r.status_recibo = 'Emitido'
              AND r.id_alumno = ?
              AND rd.id_producto = ?
              AND rd.mes = ?
              AND rd.anio = ?
            LIMIT 1
            `,
            [
              cargo.id_alumno,
              cargo.id_producto,
              cargo.mes,
              cargo.anio
            ]
          );

          if (rows.length === 0) {

            // 3️⃣ Cancelar cargo
            await conn.execute(
              `
              UPDATE alumnos_cargos
              SET status_cargo = 'Cancelado'
              WHERE id_cargo = ?
              `,
              [cargo.id_cargo]
            );

            cancelados++;
          }
        }

        return {
          procesados: cargos.length,
          cancelados
        };
      });

      res.json({ ok: true, ...resultado });

    } catch (error) {
      next(error);

    } finally {

      // 🔥 LIMPIEZA GARANTIZADA (fuera de transacción)
      try {
        await pool.execute(
          `
          UPDATE alumnos_cargos
          SET solicita_cancelacion = 0
          WHERE solicita_cancelacion = 1
          `
        );
      } catch (cleanupError) {
        logger.error("ERROR CRÍTICO: No se pudieron limpiar flags de cancelación", {
          error: cleanupError.message,
          ACCION_REQUERIDA: "Revisar manualmente alumnos_cargos"
        });
      }
    }
  };
};