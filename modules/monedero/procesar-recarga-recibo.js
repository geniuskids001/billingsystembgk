const { randomUUID } = require("crypto");

module.exports = function procesarRecargaReciboFactory({
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

  async function limpiarErrorMonedero(conn, idRecibo) {
    await conn.execute(
      `
      UPDATE recibos
      SET
        error_message = CASE
          WHEN error_message LIKE 'Monedero:%' THEN NULL
          ELSE error_message
        END,
        reintentar_monedero = 0
      WHERE id_recibo = ?
      `,
      [idRecibo]
    );
  }

  async function procesarRecargaRecibo(idRecibo) {
    const idReciboLimpio = String(idRecibo || "").trim();

    if (!idReciboLimpio) {
      throw crearError("id_recibo es requerido", 400);
    }

    logger.info("Iniciando procesamiento de recarga", {
      id_recibo: idReciboLimpio
    });

    return executeInTransaction(async (conn) => {
      // =========================================================
      // 1. Recibo
      // =========================================================
      const [[recibo]] = await conn.execute(
        `
        SELECT
          id_recibo,
          id_alumno,
          id_plantel,
          id_plantel_academico,
          id_usuario,
          status_recibo
        FROM recibos
        WHERE id_recibo = ?
        FOR UPDATE
        `,
        [idReciboLimpio]
      );

      if (!recibo) {
        throw crearError("El recibo no existe", 404);
      }

      if (recibo.status_recibo !== "Emitido") {
        throw crearError(
          "El recibo debe estar emitido para acreditar la recarga",
          409
        );
      }

      if (!recibo.id_alumno) {
        throw crearError(
          "El recibo no tiene un alumno asignado",
          400
        );
      }

      const idPlantel =
        recibo.id_plantel_academico || recibo.id_plantel;

      if (!idPlantel) {
        throw crearError(
          "El recibo no tiene un plantel asignado",
          400
        );
      }

      // =========================================================
      // 2. Configuración del plantel
      // =========================================================
      const [[configuracion]] = await conn.execute(
        `
        SELECT id_producto_recarga
        FROM monedero_configuracion_plantel
        WHERE id_plantel = ?
          AND status = 'Activo'
        LIMIT 1
        `,
        [idPlantel]
      );

      if (!configuracion?.id_producto_recarga) {
        throw crearError(
          "El plantel no tiene configurado un producto de recarga",
          409
        );
      }

      // =========================================================
      // 3. Detalles de recarga
      // =========================================================
      const [detalles] = await conn.execute(
        `
        SELECT
          id_detalle,
          nombre_producto,
          precio_final
        FROM recibos_detalle
        WHERE id_recibo = ?
          AND id_producto = ?
          AND status_detalle = 'Emitido'
        ORDER BY id_detalle
        FOR UPDATE
        `,
        [
          idReciboLimpio,
          configuracion.id_producto_recarga
        ]
      );

      if (detalles.length === 0) {
        throw crearError(
          "El recibo no contiene detalles emitidos de recarga",
          409
        );
      }

      let montoTotal = 0;

      for (const detalle of detalles) {
        const importe = Number(detalle.precio_final);

        if (!Number.isFinite(importe) || importe <= 0) {
          throw crearError(
            `El detalle ${detalle.id_detalle} tiene un precio_final inválido`,
            400
          );
        }

        montoTotal += importe;
      }

      montoTotal = Number(montoTotal.toFixed(2));

      // =========================================================
      // 4. Idempotencia
      // =========================================================
      const [[movimientoExistente]] = await conn.execute(
        `
        SELECT id_movimiento
        FROM monedero_movimientos
        WHERE id_recibo_origen = ?
          AND tipo_movimiento = 'Recarga'
        LIMIT 1
        `,
        [idReciboLimpio]
      );

      if (movimientoExistente) {
        await limpiarErrorMonedero(conn, idReciboLimpio);

        logger.info("Recarga previamente procesada", {
          id_recibo: idReciboLimpio,
          id_movimiento: movimientoExistente.id_movimiento
        });

        return {
          procesado: false,
          duplicado: true,
          id_recibo: idReciboLimpio,
          id_movimiento: movimientoExistente.id_movimiento,
          mensaje: "La recarga de este recibo ya fue procesada"
        };
      }

      const idsDetalles = detalles.map(
        ({ id_detalle }) => id_detalle
      );

      const placeholders = idsDetalles
        .map(() => "?")
        .join(",");

      const [detallesExistentes] = await conn.execute(
        `
        SELECT id_detalle_recibo_origen
        FROM monedero_movimiento_detalles
        WHERE id_detalle_recibo_origen IN (${placeholders})
        LIMIT 1
        `,
        idsDetalles
      );

      if (detallesExistentes.length > 0) {
        throw crearError(
          "Uno de los detalles de recarga ya fue procesado",
          409
        );
      }

      // =========================================================
      // 5. Cuenta
      // =========================================================
      const [[cuenta]] = await conn.execute(
        `
        SELECT
          mc.id_cuenta,
          mc.saldo_actual,
          mc.status AS status_cuenta,
          mca.status AS status_relacion
        FROM monedero_cuenta_alumnos mca
        JOIN monedero_cuentas mc
          ON mc.id_cuenta = mca.id_cuenta
        WHERE mca.id_alumno = ?
        LIMIT 1
        FOR UPDATE
        `,
        [recibo.id_alumno]
      );

      if (!cuenta) {
        throw crearError(
          "El alumno no tiene una cuenta asignada",
          409
        );
      }

      if (cuenta.status_relacion !== "Activo") {
        throw crearError(
          "La relación del alumno con la cuenta está inactiva",
          409
        );
      }

      if (cuenta.status_cuenta !== "Activo") {
        throw crearError(
          "La cuenta del alumno está inactiva",
          409
        );
      }

      const saldoAnterior = Number(cuenta.saldo_actual);

      if (!Number.isFinite(saldoAnterior)) {
        throw crearError(
          "El saldo actual de la cuenta es inválido",
          500
        );
      }

      const saldoPosterior = Number(
        (saldoAnterior + montoTotal).toFixed(2)
      );

      // =========================================================
      // 6. Movimiento
      // =========================================================
      const idMovimiento = randomUUID();

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
          id_recibo_origen,
          concepto
        )
        VALUES (?, ?, ?, ?, ?, 'Recarga', ?, ?, ?, ?)
        `,
        [
          idMovimiento,
          cuenta.id_cuenta,
          recibo.id_alumno,
          idPlantel,
          recibo.id_usuario || null,
          montoTotal,
          saldoAnterior,
          idReciboLimpio,
          `Recarga desde recibo ${idReciboLimpio}`
        ]
      );

      for (const detalle of detalles) {
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
          VALUES (?, ?, NULL, ?, NULL, ?, 0, 1, ?)
          `,
          [
            randomUUID(),
            idMovimiento,
            detalle.id_detalle,
            detalle.nombre_producto || "Recarga de saldo",
            Number(detalle.precio_final)
          ]
        );
      }

      // =========================================================
      // 7. Actualizar saldo
      // =========================================================
      const [actualizacion] = await conn.execute(
        `
        UPDATE monedero_cuentas
        SET saldo_actual = ?
        WHERE id_cuenta = ?
          AND status = 'Activo'
        `,
        [saldoPosterior, cuenta.id_cuenta]
      );

      if (actualizacion.affectedRows !== 1) {
        throw crearError(
          "No fue posible actualizar el saldo de la cuenta",
          500
        );
      }

      await limpiarErrorMonedero(conn, idReciboLimpio);

      logger.info("Recarga procesada correctamente", {
        id_recibo: idReciboLimpio,
        id_movimiento: idMovimiento,
        id_cuenta: cuenta.id_cuenta,
        id_alumno: recibo.id_alumno,
        monto: montoTotal,
        saldo_anterior: saldoAnterior,
        saldo_posterior: saldoPosterior,
        cantidad_detalles: detalles.length
      });

      return {
        procesado: true,
        id_recibo: idReciboLimpio,
        id_movimiento: idMovimiento,
        id_cuenta: cuenta.id_cuenta,
        monto: montoTotal,
        saldo_anterior: saldoAnterior,
        saldo_posterior: saldoPosterior,
        cantidad_detalles: detalles.length
      };
    });
  }

  async function guardarErrorRecibo(idRecibo, error) {
    const idReciboLimpio = String(idRecibo || "").trim();

    const mensajeOriginal = String(
      error?.message ||
      "Monedero: Error desconocido al procesar la recarga"
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
        SET
          error_message = ?,
         reintentar_monedero = 0
        WHERE id_recibo = ?
        `,
        [mensaje, idReciboLimpio]
      );
    } catch (updateError) {
      logger.error("No se pudo guardar el error de monedero", {
        id_recibo: idReciboLimpio,
        error_original: mensaje,
        error_update: updateError.message
      });
    }

    return mensaje;
  }

  async function procesarRecargaReciboHandler(req, res) {
    const idRecibo = String(
      req.body?.id_recibo || ""
    ).trim();

    const startTime = Date.now();

    if (!idRecibo) {
      return res.status(400).json({
        ok: false,
        error: "Monedero: id_recibo es requerido"
      });
    }

    try {
      const resultado = await procesarRecargaRecibo(idRecibo);

      return res.json({
        ok: true,
        ...resultado,
        duration_ms: Date.now() - startTime
      });
    } catch (error) {
      const mensaje = await guardarErrorRecibo(
        idRecibo,
        error
      );

      logger.error("Error procesando recarga de monedero", {
        id_recibo: idRecibo,
        error: mensaje,
        status_code: error.statusCode || 500
      });

      return res
        .status(error.statusCode || 500)
        .json({
          ok: false,
          error: mensaje,
          duration_ms: Date.now() - startTime
        });
    }
  }

  return {
    procesarRecargaRecibo,
    procesarRecargaReciboHandler,
    guardarErrorRecibo,

    // Alias para conservar compatibilidad con emitir-recibo
    guardarErrorRecargaRecibo: guardarErrorRecibo
  };
};