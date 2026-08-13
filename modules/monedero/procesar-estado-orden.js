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
          "No pudimos identificar la orden. Actualiza la pantalla e inténtalo nuevamente.",
          400
        );
      }

      if (
        ![
          "Pedido",
          "Preparado",
          "Entregado",
          "Cancelado"
        ].includes(statusOrden)
      ) {
        throw crearError(
          "Selecciona un estado válido para la orden.",
          400
        );
      }

      if (!idUsuario) {
        throw crearError(
          "No pudimos identificar tu sesión. Vuelve a iniciar sesión e inténtalo nuevamente.",
          401
        );
      }

      // =====================================================
      // 2. TRANSACCIÓN
      // =====================================================

      const resultado =
        await executeInTransaction(
          async (conn) => {

            // =================================================
            // 2.1 BLOQUEAR Y OBTENER ORDEN
            // =================================================

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
                "Esta orden ya no está disponible. Actualiza la pantalla para ver la información más reciente.",
                404
              );
            }

            // =================================================
            // 2.2 IDEMPOTENCIA / DOBLE TAP
            // =================================================

            if (
              orden.status_orden ===
              statusOrden
            ) {

              return {
                id_orden: idOrden,
                status_anterior:
                  orden.status_orden,
                status_orden:
                  statusOrden,
                procesado: false,
                duplicado: true,
                mensaje:
                  `La orden ya se encuentra como ${statusOrden}.`
              };
            }

            // =================================================
            // 2.3 CAMBIAR A PEDIDO
            //
            // Regresar a Pedido significa reiniciar el flujo
            // operativo de cocina.
            // =================================================

            if (statusOrden === "Pedido") {

              await conn.execute(
                `
                UPDATE monedero_ordenes
                SET
                  status_orden = 'Pedido',

                  fecha_preparacion = NULL,
                  id_usuario_preparacion = NULL,

                  fecha_entrega = NULL,
                  id_usuario_entrega = NULL,

                  fecha_cancelacion = NULL,
                  id_usuario_cancelacion = NULL

                WHERE id_orden = ?
                `,
                [idOrden]
              );
            }

            // =================================================
            // 2.4 CAMBIAR A PREPARADO
            //
            // Si venía de Entregado, se elimina la entrega
            // porque fue corregida.
            // =================================================

            else if (
              statusOrden === "Preparado"
            ) {

              await conn.execute(
                `
                UPDATE monedero_ordenes
                SET
                  status_orden = 'Preparado',

                  fecha_preparacion = NOW(),
                  id_usuario_preparacion = ?,

                  fecha_entrega = NULL,
                  id_usuario_entrega = NULL,

                  fecha_cancelacion = NULL,
                  id_usuario_cancelacion = NULL

                WHERE id_orden = ?
                `,
                [
                  idUsuario,
                  idOrden
                ]
              );
            }

            // =================================================
            // 2.5 CAMBIAR A ENTREGADO
            //
            // Conservamos fecha_preparacion si ya existía.
            // =================================================

            else if (
              statusOrden === "Entregado"
            ) {

              await conn.execute(
                `
                UPDATE monedero_ordenes
                SET
                  status_orden = 'Entregado',

                  fecha_entrega = NOW(),
                  id_usuario_entrega = ?,

                  fecha_cancelacion = NULL,
                  id_usuario_cancelacion = NULL

                WHERE id_orden = ?
                `,
                [
                  idUsuario,
                  idOrden
                ]
              );
            }

            // =================================================
            // 2.6 CAMBIAR A CANCELADO
            // =================================================

            else if (
              statusOrden === "Cancelado"
            ) {

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

            // =================================================
            // 2.7 LOG
            // =================================================

            logger.info(
              "Estado de orden Genius Bites actualizado",
              {
                id_orden:
                  idOrden,

                status_anterior:
                  orden.status_orden,

                status_nuevo:
                  statusOrden,

                id_usuario:
                  idUsuario
              }
            );

            return {
              id_orden:
                idOrden,

              status_anterior:
                orden.status_orden,

              status_orden:
                statusOrden,

              procesado:
                true,

              duplicado:
                false,

              mensaje:
                `Orden actualizada a ${statusOrden}.`
            };
          }
        );

      // =====================================================
      // 3. RESPUESTA
      // =====================================================

      return res
        .status(200)
        .json({
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
          : "Monedero: No pudimos actualizar la orden. Inténtalo nuevamente.";

      logger.error(
        "Error actualizando orden Genius Bites",
        {
          id_orden:
            req.body?.id_orden ||
            null,

          status_solicitado:
            req.body?.status_orden ||
            null,

          id_usuario:
            req.usuario?.id_usuario ||
            null,

          error:
            error.message,

          status_code:
            error.statusCode || 500,

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
          error: mensaje,
          duration_ms:
            Date.now() - startTime
        });
    }
  };
};