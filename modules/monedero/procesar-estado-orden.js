module.exports = function procesarEstadoOrdenFactory({
  executeInTransaction,
  logger
}) {

  function crearError(mensaje, statusCode = 400) {
    const error = new Error(
      mensaje.startsWith("Monedero:")
        ? mensaje
        : `Monedero: ${mensaje}`
    );

    error.statusCode = statusCode;
    return error;
  }

  return async function procesarEstadoOrdenHandler(req, res) {
    const startTime = Date.now();

    try {
      const idOrden = String(
        req.body?.id_orden || ""
      ).trim();

      const statusOrden = String(
        req.body?.status_orden || ""
      ).trim();

      const idUsuario =
        req.usuario?.id_usuario;

      // =====================================================
      // 1. VALIDAR
      // =====================================================

      if (!idOrden) {
        throw crearError(
          "id_orden es requerido",
          400
        );
      }

      if (
        ![
          "Preparado",
          "Entregado",
          "Cancelado"
        ].includes(statusOrden)
      ) {
        throw crearError(
          "Estado de orden no válido",
          400
        );
      }

      if (!idUsuario) {
        throw crearError(
          "No fue posible identificar al usuario",
          401
        );
      }

      // =====================================================
      // 2. TRANSACCIÓN
      // =====================================================

      const resultado =
        await executeInTransaction(
          async (conn) => {

            const [[orden]] =
              await conn.execute(
                `
                SELECT
                  id_orden,
                  status_orden
                FROM monedero_ordenes
                WHERE id_orden = ?
                FOR UPDATE
                `,
                [idOrden]
              );

            if (!orden) {
              throw crearError(
                "La orden no existe",
                404
              );
            }

            // Ya está en ese estado:
            // respuesta idempotente, no duplicamos nada.
            if (
              orden.status_orden ===
              statusOrden
            ) {
              return {
                id_orden: idOrden,
                status_orden: statusOrden,
                procesado: false,
                duplicado: true
              };
            }

            // =================================================
            // 3. ACTUALIZAR ESTADO + USUARIO + FECHA
            // =================================================

            if (statusOrden === "Preparado") {

              await conn.execute(
                `
                UPDATE monedero_ordenes
                SET
                  status_orden = 'Preparado',
                  fecha_preparacion = NOW(),
                  id_usuario_preparacion = ?
                WHERE id_orden = ?
                `,
                [
                  idUsuario,
                  idOrden
                ]
              );

            } else if (
              statusOrden === "Entregado"
            ) {

              await conn.execute(
                `
                UPDATE monedero_ordenes
                SET
                  status_orden = 'Entregado',
                  fecha_entrega = NOW(),
                  id_usuario_entrega = ?
                WHERE id_orden = ?
                `,
                [
                  idUsuario,
                  idOrden
                ]
              );

            } else {

              await conn.execute(
                `
                UPDATE monedero_ordenes
                SET
                  status_orden = 'Cancelado',
                  fecha_cancelacion = NOW(),
                  id_usuario_cancelacion = ?
                WHERE id_orden = ?
                `,
                [
                  idUsuario,
                  idOrden
                ]
              );
            }

            logger.info(
              "Estado de orden Genius Bites actualizado",
              {
                id_orden: idOrden,
                status_anterior:
                  orden.status_orden,
                status_nuevo:
                  statusOrden,
                id_usuario:
                  idUsuario
              }
            );

            return {
              id_orden: idOrden,
              status_anterior:
                orden.status_orden,
              status_orden:
                statusOrden,
              procesado: true,
              duplicado: false
            };
          }
        );

      return res.status(200).json({
        ok: true,
        ...resultado,
        duration_ms:
          Date.now() - startTime
      });

    } catch (error) {

      const mensaje =
        error.message?.startsWith(
          "Monedero:"
        )
          ? error.message
          : "Monedero: No fue posible actualizar la orden";

      logger.error(
        "Error actualizando orden Genius Bites",
        {
          id_orden:
            req.body?.id_orden || null,
          id_usuario:
            req.usuario?.id_usuario ||
            null,
          error:
            error.message,
          duration_ms:
            Date.now() - startTime
        }
      );

      return res
        .status(
          error.statusCode || 500
        )
        .json({
          ok: false,
          error: mensaje
        });
    }
  };
};