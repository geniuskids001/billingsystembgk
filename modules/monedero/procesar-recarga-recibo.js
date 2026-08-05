const { randomUUID } = require("crypto");

module.exports = function procesarRecargaReciboFactory({
  pool,
  executeInTransaction,
  logger
}) {
  /**
   * Procesa todos los detalles de recarga de un recibo.
   *
   * Regla:
   * - Recibe únicamente id_recibo.
   * - Obtiene alumno, plantel, usuario, cuenta y configuración desde BD.
   * - Suma todos los precio_final correspondientes al producto de recarga.
   * - Toda la recarga se registra en una sola transacción.
   */
  async function procesarRecargaRecibo(idRecibo) {
    if (!idRecibo) {
      const error = new Error("Monedero: id_recibo es requerido");
      error.statusCode = 400;
      throw error;
    }

    return executeInTransaction(async (conn) => {
      // ============================================================
      // 1. Bloquear y validar el recibo
      // ============================================================
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
        [idRecibo]
      );

      if (!recibo) {
        const error = new Error("Monedero: El recibo no existe");
        error.statusCode = 404;
        throw error;
      }

      if (recibo.status_recibo !== "Emitido") {
        const error = new Error(
          "Monedero: El recibo debe estar emitido para acreditar la recarga"
        );
        error.statusCode = 409;
        throw error;
      }

      if (!recibo.id_alumno) {
        const error = new Error(
          "Monedero: El recibo no tiene un alumno asignado"
        );
        error.statusCode = 400;
        throw error;
      }

      const idPlantel =
        recibo.id_plantel_academico || recibo.id_plantel;

      if (!idPlantel) {
        const error = new Error(
          "Monedero: El recibo no tiene un plantel asignado"
        );
        error.statusCode = 400;
        throw error;
      }

      // ============================================================
      // 2. Obtener configuración activa del plantel
      // ============================================================
      const [[configuracion]] = await conn.execute(
        `
        SELECT
          id_producto_recarga
        FROM monedero_configuracion_plantel
        WHERE id_plantel = ?
          AND status = 'Activo'
        LIMIT 1
        `,
        [idPlantel]
      );

      if (!configuracion) {
        const error = new Error(
          "Monedero: El plantel no tiene una configuración activa"
        );
        error.statusCode = 409;
        throw error;
      }

      // ============================================================
      // 3. Obtener todos los renglones de recarga
      // ============================================================
      const [detallesRecarga] = await conn.execute(
        `
        SELECT
          id_detalle,
          id_producto,
          nombre_producto,
          precio_final,
          status_detalle
        FROM recibos_detalle
        WHERE id_recibo = ?
          AND id_producto = ?
          AND status_detalle = 'Emitido'
        ORDER BY id_detalle
        FOR UPDATE
        `,
        [
          idRecibo,
          configuracion.id_producto_recarga
        ]
      );

      /*
       * Esto permite llamar la función desde emitir-recibo aunque el recibo
       * no contenga una recarga: simplemente no hace nada.
       */
      if (detallesRecarga.length === 0) {
        return {
          procesado: false,
          contiene_recarga: false,
          id_recibo: idRecibo,
          mensaje: "El recibo no contiene productos de recarga"
        };
      }

      // ============================================================
      // 4. Validar importes
      // ============================================================
      let montoTotal = 0;

      for (const detalle of detallesRecarga) {
        const importe = Number(detalle.precio_final);

        if (!Number.isFinite(importe) || importe <= 0) {
          const error = new Error(
            `Monedero: El detalle ${detalle.id_detalle} tiene un precio_final inválido`
          );
          error.statusCode = 400;
          throw error;
        }

        montoTotal += importe;
      }

      montoTotal = Number(montoTotal.toFixed(2));

      if (montoTotal <= 0) {
        const error = new Error(
          "Monedero: El importe total de la recarga debe ser mayor a cero"
        );
        error.statusCode = 400;
        throw error;
      }

      // ============================================================
      // 5. Evitar procesar dos veces el mismo recibo
      // ============================================================
      const [[movimientoExistente]] = await conn.execute(
        `
        SELECT id_movimiento
        FROM monedero_movimientos
        WHERE id_recibo_origen = ?
          AND tipo_movimiento = 'Recarga'
        LIMIT 1
        `,
        [idRecibo]
      );

      if (movimientoExistente) {
        return {
          procesado: false,
          contiene_recarga: true,
          duplicado: true,
          id_recibo: idRecibo,
          id_movimiento: movimientoExistente.id_movimiento,
          mensaje: "La recarga de este recibo ya fue procesada"
        };
      }

      // Protección adicional a nivel de detalle
      const idsDetalles = detallesRecarga.map(
        (detalle) => detalle.id_detalle
      );

      const placeholders = idsDetalles.map(() => "?").join(",");

      const [detallesProcesados] = await conn.execute(
        `
        SELECT id_detalle_recibo_origen
        FROM monedero_movimiento_detalles
        WHERE id_detalle_recibo_origen IN (${placeholders})
        LIMIT 1
        `,
        idsDetalles
      );

      if (detallesProcesados.length > 0) {
        const error = new Error(
          "Monedero: Uno de los detalles de recarga ya fue procesado"
        );
        error.statusCode = 409;
        throw error;
      }

      // ============================================================
      // 6. Obtener la cuenta activa del alumno
      // ============================================================
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
        const error = new Error(
          "Monedero: El alumno no tiene una cuenta asignada"
        );
        error.statusCode = 409;
        throw error;
      }

      if (cuenta.status_relacion !== "Activo") {
        const error = new Error(
          "Monedero: La relación del alumno con la cuenta está inactiva"
        );
        error.statusCode = 409;
        throw error;
      }

      if (cuenta.status_cuenta !== "Activo") {
        const error = new Error(
          "Monedero: La cuenta del alumno está inactiva"
        );
        error.statusCode = 409;
        throw error;
      }

      const saldoAnterior = Number(cuenta.saldo_actual);

      if (!Number.isFinite(saldoAnterior)) {
        const error = new Error(
          "Monedero: El saldo actual de la cuenta es inválido"
        );
        error.statusCode = 500;
        throw error;
      }

      const saldoPosterior = Number(
        (saldoAnterior + montoTotal).toFixed(2)
      );

      // ============================================================
      // 7. Crear cabecera del movimiento
      // ============================================================
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
          idRecibo,
          `Recarga desde recibo ${idRecibo}`
        ]
      );

      // ============================================================
      // 8. Crear un detalle por cada renglón de recarga
      // ============================================================
      for (const detalle of detallesRecarga) {
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

      // ============================================================
      // 9. Actualizar saldo materializado
      // ============================================================
      const [actualizacionCuenta] = await conn.execute(
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

      if (actualizacionCuenta.affectedRows !== 1) {
        const error = new Error(
          "Monedero: No fue posible actualizar el saldo de la cuenta"
        );
        error.statusCode = 500;
        throw error;
      }

      // ============================================================
      // 10. Limpiar error y bandera de reintento
      // ============================================================
      await conn.execute(
        `
        UPDATE recibos
        SET
          error_message = CASE
            WHEN error_message LIKE 'Monedero:%' THEN NULL
            ELSE error_message
          END,
          reintentar_recarga_monedero = 0
        WHERE id_recibo = ?
        `,
        [idRecibo]
      );

      logger.info("Recarga de monedero procesada", {
        id_recibo: idRecibo,
        id_movimiento: idMovimiento,
        id_cuenta: cuenta.id_cuenta,
        id_alumno: recibo.id_alumno,
        cantidad_detalles: detallesRecarga.length,
        monto_total: montoTotal,
        saldo_anterior: saldoAnterior,
        saldo_posterior: saldoPosterior
      });

      return {
        procesado: true,
        contiene_recarga: true,
        id_recibo: idRecibo,
        id_movimiento: idMovimiento,
        id_cuenta: cuenta.id_cuenta,
        monto: montoTotal,
        saldo_anterior: saldoAnterior,
        saldo_posterior: saldoPosterior,
        cantidad_detalles: detallesRecarga.length
      };
    });
  }

  /**
   * Guarda un error legible para AppSheet.
   */
  async function guardarErrorRecibo(idRecibo, error) {
    const mensajeOriginal = String(
      error?.message || "Error desconocido al procesar la recarga"
    );

    const mensaje = mensajeOriginal.startsWith("Monedero:")
      ? mensajeOriginal
      : `Monedero: ${mensajeOriginal}`;

    const mensajeSeguro = mensaje.substring(0, 255);

    try {
      await pool.execute(
        `
        UPDATE recibos
        SET
          error_message = ?,
          reintentar_recarga_monedero = 0
        WHERE id_recibo = ?
        `,
        [
          mensajeSeguro,
          idRecibo
        ]
      );
    } catch (updateError) {
      logger.error("No se pudo guardar error de monedero en recibo", {
        id_recibo: idRecibo,
        error_original: mensajeSeguro,
        error_update: updateError.message
      });
    }

    return mensajeSeguro;
  }

  /**
   * Endpoint manual/reintentable.
   */
  async function procesarRecargaReciboHandler(req, res) {
    const { id_recibo } = req.body || {};
    const startTime = Date.now();

    if (!id_recibo) {
      return res.status(400).json({
        ok: false,
        error: "Monedero: id_recibo es requerido"
      });
    }

    try {
      const resultado = await procesarRecargaRecibo(
        String(id_recibo).trim()
      );

      return res.json({
        ok: true,
        ...resultado,
        duration_ms: Date.now() - startTime
      });
    } catch (error) {
      const mensaje = await guardarErrorRecibo(
        String(id_recibo).trim(),
        error
      );

      logger.error("Error procesando recarga de monedero", {
        id_recibo,
        error: mensaje,
        stack: error.stack
      });

      return res.status(error.statusCode || 500).json({
        ok: false,
        error: mensaje,
        duration_ms: Date.now() - startTime
      });
    }
  }

  return {
    procesarRecargaRecibo,
    procesarRecargaReciboHandler,
    guardarErrorRecibo
  };
};