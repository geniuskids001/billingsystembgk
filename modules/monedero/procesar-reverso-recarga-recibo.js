const { randomUUID } = require("crypto");

module.exports = function procesarReversoRecargaReciboFactory({
  pool,
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

  async function guardarErrorRecibo(idRecibo, error) {
    const idReciboLimpio = String(
      idRecibo || ""
    ).trim();

    const mensajeOriginal = String(
      error?.message ||
      "Monedero: Error desconocido al reversar la recarga"
    );

    const mensaje = (
      mensajeOriginal.startsWith("Monedero:")
        ? mensajeOriginal
        : `Monedero: ${mensajeOriginal}`
    ).substring(0, 255);

    try {
      await pool.execute(
        `
        UPDATE recibos
        SET error_message = ?
        WHERE id_recibo = ?
        `,
        [
          mensaje,
          idReciboLimpio
        ]
      );

    } catch (updateError) {

      logger.error(
        "No se pudo guardar el error del reverso de recarga",
        {
          id_recibo: idReciboLimpio,
          error_original: mensaje,
          error_update: updateError.message
        }
      );
    }

    return mensaje;
  }

  async function limpiarErrorMonedero(
    conn,
    idRecibo
  ) {

    await conn.execute(
      `
      UPDATE recibos
      SET error_message = CASE
        WHEN error_message LIKE 'Monedero:%'
          THEN NULL
        ELSE error_message
      END
      WHERE id_recibo = ?
      `,
      [idRecibo]
    );
  }

  async function procesarReversoRecargaRecibo(
    idRecibo
  ) {

    const idReciboLimpio = String(
      idRecibo || ""
    ).trim();

    if (!idReciboLimpio) {
      throw crearError(
        "id_recibo es requerido",
        400
      );
    }

    logger.info(
      "Iniciando reverso de recarga",
      {
        id_recibo: idReciboLimpio
      }
    );

    return executeInTransaction(
      async (conn) => {

        // =====================================================
        // 1. RECIBO
        // =====================================================

        const [[recibo]] =
          await conn.execute(
            `
            SELECT
              id_recibo,
              status_recibo
            FROM recibos
            WHERE id_recibo = ?
            FOR UPDATE
            `,
            [idReciboLimpio]
          );

        if (!recibo) {
          throw crearError(
            "El recibo no existe",
            404
          );
        }

        if (
          recibo.status_recibo !==
          "Cancelado"
        ) {
          throw crearError(
            "El recibo debe estar cancelado para reversar una recarga",
            409
          );
        }

        // =====================================================
        // 2. BUSCAR RECARGA REAL
        //
        // Si nunca existió movimiento Recarga,
        // no existe saldo que reversar.
        // =====================================================

        const [[recarga]] =
          await conn.execute(
            `
            SELECT
              id_movimiento,
              id_cuenta,
              id_alumno,
              id_plantel,
              id_usuario,
              monto
            FROM monedero_movimientos
            WHERE id_recibo_origen = ?
              AND tipo_movimiento = 'Recarga'
            ORDER BY fecha_movimiento ASC
            LIMIT 1
            FOR UPDATE
            `,
            [idReciboLimpio]
          );

        if (!recarga) {

          // El recibo ya está cancelado y nunca
          // afectó saldo. El error de Monedero
          // deja de requerir atención.
          await limpiarErrorMonedero(
            conn,
            idReciboLimpio
          );

          logger.info(
            "Recibo cancelado sin recarga real; reverso no aplica",
            {
              id_recibo:
                idReciboLimpio
            }
          );

          return {
            procesado: false,
            aplica: false,
            duplicado: false,
            id_recibo:
              idReciboLimpio,
            mensaje:
              "El recibo no generó una recarga de monedero; no hay saldo que reversar"
          };
        }

        const montoRecarga =
          Number(recarga.monto);

        if (
          !Number.isFinite(montoRecarga) ||
          montoRecarga <= 0
        ) {
          throw crearError(
            "La recarga original tiene un monto inválido",
            500
          );
        }

        // =====================================================
        // 3. IDEMPOTENCIA
        //
        // Si ya existe ReversoRecarga de esta
        // Recarga, no volver a modificar saldo.
        // =====================================================

        const [[reversoExistente]] =
          await conn.execute(
            `
            SELECT
              id_movimiento,
              monto,
              saldo_anterior,
              saldo_posterior
            FROM monedero_movimientos
            WHERE tipo_movimiento =
                  'ReversoRecarga'
              AND id_movimiento_origen = ?
            LIMIT 1
            `,
            [
              recarga.id_movimiento
            ]
          );

        if (reversoExistente) {

          await limpiarErrorMonedero(
            conn,
            idReciboLimpio
          );

          logger.info(
            "Reverso de recarga previamente procesado",
            {
              id_recibo:
                idReciboLimpio,
              id_movimiento_recarga:
                recarga.id_movimiento,
              id_movimiento_reverso:
                reversoExistente
                  .id_movimiento
            }
          );

          return {
            procesado: false,
            aplica: true,
            duplicado: true,

            id_recibo:
              idReciboLimpio,

            id_movimiento_recarga:
              recarga.id_movimiento,

            id_movimiento_reverso:
              reversoExistente
                .id_movimiento,

            monto:
              Number(
                reversoExistente.monto
              ),

            saldo_anterior:
              Number(
                reversoExistente
                  .saldo_anterior
              ),

            saldo_posterior:
              Number(
                reversoExistente
                  .saldo_posterior
              ),

            mensaje:
              "El reverso de esta recarga ya fue procesado"
          };
        }

        // =====================================================
        // 4. CUENTA ORIGINAL
        // =====================================================

        const [[cuenta]] =
          await conn.execute(
            `
            SELECT
              id_cuenta,
              saldo_actual
            FROM monedero_cuentas
            WHERE id_cuenta = ?
            FOR UPDATE
            `,
            [
              recarga.id_cuenta
            ]
          );

        if (!cuenta) {
          throw crearError(
            "La cuenta original de la recarga ya no existe",
            500
          );
        }

        const saldoAnterior =
          Number(cuenta.saldo_actual);

        if (
          !Number.isFinite(
            saldoAnterior
          )
        ) {
          throw crearError(
            "El saldo actual de la cuenta es inválido",
            500
          );
        }

        const montoReverso =
          Number(
            (
              -Math.abs(montoRecarga)
            ).toFixed(2)
          );

        const saldoPosterior =
          Number(
            (
              saldoAnterior +
              montoReverso
            ).toFixed(2)
          );

        // =====================================================
        // 5. CREAR MOVIMIENTO REVERSO
        // =====================================================

        const idMovimientoReverso =
          randomUUID();

        await conn.execute(
          `
          INSERT INTO monedero_movimientos (
            id_movimiento,
            id_cuenta,
            id_alumno,
            id_plantel,
            id_usuario,
            tipo_movimiento,
            monto,
            saldo_anterior,
            id_movimiento_origen,
            id_recibo_origen,
            concepto
          )
          VALUES (
            ?, ?, ?, ?, ?,
            'ReversoRecarga',
            ?, ?, ?, ?, ?
          )
          `,
          [
            idMovimientoReverso,
            recarga.id_cuenta,
            recarga.id_alumno,
            recarga.id_plantel,
            recarga.id_usuario || null,
            montoReverso,
            saldoAnterior,
            recarga.id_movimiento,
            idReciboLimpio,
            (
              "Reverso de recarga por cancelación " +
              `del recibo ${idReciboLimpio}`
            ).substring(0, 255)
          ]
        );

        // =====================================================
        // 6. COPIAR DETALLES PARA AUDITORÍA
        // =====================================================

        const [detallesRecarga] =
          await conn.execute(
            `
            SELECT
              id_movimiento_detalle,
              id_detalle_recibo_origen,
              nombre_producto,
              cantidad,
              precio_unitario
            FROM monedero_movimiento_detalles
            WHERE id_movimiento = ?
            ORDER BY id_movimiento_detalle
            `,
            [
              recarga.id_movimiento
            ]
          );

        for (
          const detalle
          of detallesRecarga
        ) {

          await conn.execute(
            `
            INSERT INTO monedero_movimiento_detalles (
              id_movimiento_detalle,
              id_movimiento,
              id_movimiento_detalle_origen,
              id_detalle_recibo_origen,
              id_producto_monedero,
              nombre_producto,
              requiere_preparacion,
              cantidad,
              precio_unitario
            )
            VALUES (
              ?, ?, ?, ?, NULL,
              ?, 0, ?, ?
            )
            `,
            [
              randomUUID(),

              idMovimientoReverso,

              detalle
                .id_movimiento_detalle,

              detalle
                .id_detalle_recibo_origen ||
                null,

              detalle.nombre_producto ||
                "Recarga de saldo",

              Number(
                detalle.cantidad
              ),

              Number(
                detalle.precio_unitario
              )
            ]
          );
        }

        // =====================================================
        // 7. ACTUALIZAR SALDO
        // =====================================================

        const [actualizacion] =
          await conn.execute(
            `
            UPDATE monedero_cuentas
            SET saldo_actual = ?
            WHERE id_cuenta = ?
            `,
            [
              saldoPosterior,
              recarga.id_cuenta
            ]
          );

        if (
          actualizacion.affectedRows !== 1
        ) {
          throw crearError(
            "No fue posible actualizar el saldo de la cuenta",
            500
          );
        }

        // Operación completa:
        // cualquier error Monedero previo
        // asociado al recibo deja de aplicar.
        await limpiarErrorMonedero(
          conn,
          idReciboLimpio
        );

        logger.info(
          "Reverso de recarga procesado correctamente",
          {
            id_recibo:
              idReciboLimpio,

            id_movimiento_recarga:
              recarga.id_movimiento,

            id_movimiento_reverso:
              idMovimientoReverso,

            id_cuenta:
              recarga.id_cuenta,

            id_alumno:
              recarga.id_alumno,

            monto_recarga:
              montoRecarga,

            monto_reverso:
              montoReverso,

            saldo_anterior:
              saldoAnterior,

            saldo_posterior:
              saldoPosterior,

            cantidad_detalles:
              detallesRecarga.length
          }
        );

        return {
          procesado: true,
          aplica: true,
          duplicado: false,

          id_recibo:
            idReciboLimpio,

          id_movimiento_recarga:
            recarga.id_movimiento,

          id_movimiento_reverso:
            idMovimientoReverso,

          id_cuenta:
            recarga.id_cuenta,

          monto:
            montoReverso,

          saldo_anterior:
            saldoAnterior,

          saldo_posterior:
            saldoPosterior,

          cantidad_detalles:
            detallesRecarga.length
        };
      }
    );
  }

  async function procesarReversoRecargaReciboHandler(
    req,
    res
  ) {

    const idRecibo = String(
      req.body?.id_recibo || ""
    ).trim();

    const startTime =
      Date.now();

    if (!idRecibo) {
      return res.status(400).json({
        ok: false,
        error:
          "Monedero: id_recibo es requerido"
      });
    }

    try {

      const resultado =
        await procesarReversoRecargaRecibo(
          idRecibo
        );

      return res.json({
        ok: true,
        ...resultado,
        duration_ms:
          Date.now() - startTime
      });

    } catch (error) {

      const mensaje =
        await guardarErrorRecibo(
          idRecibo,
          error
        );

      logger.error(
        "Error procesando reverso de recarga de monedero",
        {
          id_recibo:
            idRecibo,

          error:
            mensaje,

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
  }

  return {
    procesarReversoRecargaRecibo,
    procesarReversoRecargaReciboHandler,
    guardarErrorRecibo
  };
};