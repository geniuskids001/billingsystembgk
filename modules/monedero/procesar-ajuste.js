const { randomUUID } = require("crypto");

module.exports = function procesarAjusteFactory({
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

  return async function procesarAjusteHandler(req, res) {
    const startTime = Date.now();

    try {
      const idAlumno = String(
        req.body?.id_alumno || ""
      ).trim();

      const monto = Number(
        req.body?.monto
      );

      const motivo = String(
        req.body?.motivo || ""
      ).trim();

      const idUsuario =
        req.usuario?.id_usuario;

      const idPlantelUsuario =
        req.usuario?.id_plantel || null;

      // =====================================================
      // 1. VALIDAR
      // =====================================================

      if (!idAlumno) {
        throw crearError(
          "Selecciona un alumno",
          400
        );
      }

      if (
        !Number.isFinite(monto) ||
        monto === 0
      ) {
        throw crearError(
          "El monto del ajuste debe ser diferente de cero",
          400
        );
      }

      if (!motivo) {
        throw crearError(
          "El motivo del ajuste es requerido",
          400
        );
      }

      if (motivo.length > 200) {
        throw crearError(
          "El motivo es demasiado largo",
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

            // -----------------------------------------------
            // 2.1 Resolver cuenta activa del alumno
            // -----------------------------------------------

            const [[cuenta]] =
              await conn.execute(
                `
                SELECT
                  mc.id_cuenta,
                  mc.saldo_actual,
                  mc.status AS status_cuenta,
                  mca.status AS status_relacion,
                  a.id_plantel_academico AS id_plantel

                FROM monedero_cuenta_alumnos mca

                JOIN monedero_cuentas mc
                  ON mc.id_cuenta = mca.id_cuenta

                JOIN alumnos a
                  ON a.id_alumno = mca.id_alumno

                WHERE mca.id_alumno = ?
                LIMIT 1
                FOR UPDATE
                `,
                [idAlumno]
              );

            if (!cuenta) {
              throw crearError(
                "El alumno no tiene una cuenta de Genius Bites",
                404
              );
            }

            if (
              cuenta.status_relacion !== "Activo"
            ) {
              throw crearError(
                "La relación del alumno con la cuenta está inactiva",
                409
              );
            }

            if (
              cuenta.status_cuenta !== "Activo"
            ) {
              throw crearError(
                "La cuenta del alumno está inactiva",
                409
              );
            }

            // Usuario limitado a plantel
            if (
              idPlantelUsuario &&
              idPlantelUsuario !==
                cuenta.id_plantel
            ) {
              throw crearError(
                "No tienes autorización para ajustar esta cuenta",
                403
              );
            }

            const saldoAnterior =
              Number(cuenta.saldo_actual);

            const saldoPosterior =
              Number(
                (
                  saldoAnterior + monto
                ).toFixed(2)
              );

            // -----------------------------------------------
            // 2.2 Crear movimiento
            // -----------------------------------------------

            const idMovimiento =
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
                concepto
              )
              VALUES (
                ?, ?, ?, ?, ?,
                'Ajuste',
                ?, ?, ?
              )
              `,
              [
                idMovimiento,
                cuenta.id_cuenta,
                idAlumno,
                cuenta.id_plantel,
                idUsuario,
                monto,
                saldoAnterior,
                `Ajuste administrativo: ${motivo}`
                  .substring(0, 255)
              ]
            );

            // -----------------------------------------------
            // 2.3 Actualizar saldo
            // -----------------------------------------------

            const [actualizacion] =
              await conn.execute(
                `
                UPDATE monedero_cuentas
                SET saldo_actual = ?
                WHERE id_cuenta = ?
                  AND status = 'Activo'
                `,
                [
                  saldoPosterior,
                  cuenta.id_cuenta
                ]
              );

            if (
              actualizacion.affectedRows !== 1
            ) {
              throw crearError(
                "No fue posible actualizar el saldo",
                500
              );
            }

            logger.info(
              "Ajuste administrativo procesado",
              {
                id_movimiento: idMovimiento,
                id_alumno: idAlumno,
                id_cuenta: cuenta.id_cuenta,
                id_usuario: idUsuario,
                monto,
                saldo_anterior:
                  saldoAnterior,
                saldo_posterior:
                  saldoPosterior
              }
            );

            return {
              id_movimiento: idMovimiento,
              id_alumno: idAlumno,
              id_cuenta: cuenta.id_cuenta,
              monto,
              saldo_anterior:
                saldoAnterior,
              saldo_posterior:
                saldoPosterior
            };
          }
        );

      return res.status(200).json({
        ok: true,
        mensaje:
          "Ajuste realizado correctamente",
        ...resultado,
        duration_ms:
          Date.now() - startTime
      });

    } catch (error) {

      logger.error(
        "Error procesando ajuste Genius Bites",
        {
          id_alumno:
            req.body?.id_alumno || null,

          id_usuario:
            req.usuario?.id_usuario || null,

          error: error.message,

          duration_ms:
            Date.now() - startTime
        }
      );

      return res
        .status(error.statusCode || 500)
        .json({
          ok: false,
          error:
            error.message?.startsWith(
              "Monedero:"
            )
              ? error.message
              : "Monedero: No fue posible realizar el ajuste"
        });
    }
  };
};