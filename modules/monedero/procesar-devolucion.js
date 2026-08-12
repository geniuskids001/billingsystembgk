const { randomUUID } = require("crypto");

module.exports = function procesarDevolucionFactory({
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

  return async function procesarDevolucionHandler(req, res) {
    const startTime = Date.now();

    try {
      const idMovimientoCompra = String(
        req.body?.id_movimiento_compra || ""
      ).trim();

      const tipo = String(
        req.body?.tipo || ""
      ).trim();

      const detallesSolicitados = req.body?.detalles;

      const motivo = String(
        req.body?.motivo || ""
      ).trim();

      const idUsuario = req.usuario?.id_usuario;
      const idPlantelCaja = req.usuario?.id_plantel || null;

      // =========================================================
      // 1. VALIDAR PAYLOAD
      // =========================================================

      if (!idMovimientoCompra) {
        throw crearError(
          "Selecciona la compra que deseas devolver",
          400
        );
      }

      if (!["Total", "Parcial"].includes(tipo)) {
        throw crearError(
          "El tipo de devolución debe ser Total o Parcial",
          400
        );
      }

      if (!idUsuario) {
  throw crearError(
    "No fue posible identificar al usuario",
    401
  );
}

      if (motivo.length > 200) {
        throw crearError(
          "El motivo de devolución es demasiado largo",
          400
        );
      }

      if (
        tipo === "Parcial" &&
        (!Array.isArray(detallesSolicitados) ||
          detallesSolicitados.length === 0)
      ) {
        throw crearError(
          "Selecciona al menos un producto para devolver",
          400
        );
      }

      // =========================================================
      // 2. TRANSACCIÓN
      // =========================================================

      const resultado = await executeInTransaction(
        async (conn) => {

          // =====================================================
          // 2.1 COMPRA ORIGINAL
          //
          // Este lock también serializa dos devoluciones
          // simultáneas de la misma compra.
          // =====================================================

          const [[compra]] = await conn.execute(
            `
            SELECT
              id_movimiento,
              id_cuenta,
              id_alumno,
              id_plantel,
              tipo_movimiento,
              monto,
              fecha_movimiento
            FROM monedero_movimientos
            WHERE id_movimiento = ?
            FOR UPDATE
            `,
            [idMovimientoCompra]
          );

          if (!compra) {
            throw crearError(
              "La compra no existe",
              404
            );
          }

          if (compra.tipo_movimiento !== "Compra") {
            throw crearError(
              "El movimiento seleccionado no es una compra",
              409
            );
          }

          // Cajeros con plantel solamente pueden operar
          // movimientos de su propio plantel.
          // Directivos sin plantel no se bloquean.
          if (
            idPlantelCaja &&
            idPlantelCaja !== compra.id_plantel
          ) {
            throw crearError(
              "No tienes autorización para devolver compras de este plantel",
              403
            );
          }

          // =====================================================
          // 2.2 CONFIGURACIÓN DEL PLANTEL
          // =====================================================

          const [[configuracion]] = await conn.execute(
            `
            SELECT id_configuracion
            FROM monedero_configuracion_plantel
            WHERE id_plantel = ?
              AND status = 'Activo'
            LIMIT 1
            `,
            [compra.id_plantel]
          );

          if (!configuracion) {
            throw crearError(
              "Genius Bites no está activo para este plantel",
              409
            );
          }

          // =====================================================
          // 2.3 CUENTA + RELACIÓN DEL ALUMNO
          // =====================================================

          const [[cuenta]] = await conn.execute(
            `
            SELECT
              mc.id_cuenta,
              mc.saldo_actual,
              mc.status AS status_cuenta,
              mca.status AS status_relacion
            FROM monedero_cuentas mc

            JOIN monedero_cuenta_alumnos mca
              ON mca.id_cuenta = mc.id_cuenta
             AND mca.id_alumno = ?

            WHERE mc.id_cuenta = ?
            FOR UPDATE
            `,
            [
              compra.id_alumno,
              compra.id_cuenta
            ]
          );

          if (!cuenta) {
            throw crearError(
              "La cuenta original del alumno ya no está disponible",
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

          const saldoAnterior = Number(
            cuenta.saldo_actual
          );

          if (!Number.isFinite(saldoAnterior)) {
            throw crearError(
              "El saldo actual de la cuenta es inválido",
              500
            );
          }

          // =====================================================
          // 2.4 DETALLES ORIGINALES +
          //     CANTIDAD YA DEVUELTA
          // =====================================================

          const [detallesCompra] = await conn.execute(
            `
            SELECT
  d.id_movimiento_detalle,
  d.id_producto_monedero,
  d.nombre_producto,
  d.requiere_preparacion,
  d.cantidad,
  d.precio_unitario,
  d.cantidad_devuelta

FROM monedero_movimiento_detalles d
WHERE d.id_movimiento = ?
            `,
            [
              idMovimientoCompra
            ]
          );

          if (detallesCompra.length === 0) {
            throw crearError(
              "La compra no tiene productos para devolver",
              409
            );
          }

          // =====================================================
          // 2.5 DETERMINAR QUÉ SE VA A DEVOLVER
          // =====================================================

          const detallesDevolucion = [];

          if (tipo === "Total") {

            // Total significa:
            // devolver TODO LO QUE TODAVÍA NO se haya devuelto.

            for (const detalle of detallesCompra) {
              const comprada = Number(detalle.cantidad);
              const devuelta = Number(
                detalle.cantidad_devuelta || 0
              );

              const pendiente = comprada - devuelta;

              if (pendiente > 0) {
                detallesDevolucion.push({
                  ...detalle,
                  cantidad_devolver: pendiente
                });
              }
            }

          } else {

            // ===================================================
            // DEVOLUCIÓN PARCIAL
            // ===================================================

            const solicitados = new Map();

            for (const item of detallesSolicitados) {
              const idDetalle = String(
                item?.id_movimiento_detalle || ""
              ).trim();

              const cantidad = Number(item?.cantidad);

              if (
                !idDetalle ||
                !Number.isInteger(cantidad) ||
                cantidad <= 0
              ) {
                throw crearError(
                  "Uno de los productos seleccionados tiene una cantidad inválida",
                  400
                );
              }

              solicitados.set(
                idDetalle,
                (solicitados.get(idDetalle) || 0) +
                  cantidad
              );
            }

            for (
              const [idDetalle, cantidadSolicitada]
              of solicitados
            ) {
              const detalle = detallesCompra.find(
                (d) =>
                  d.id_movimiento_detalle === idDetalle
              );

              if (!detalle) {
                throw crearError(
                  "Uno de los productos no pertenece a esta compra",
                  409
                );
              }

              const comprada = Number(detalle.cantidad);
              const devuelta = Number(
                detalle.cantidad_devuelta || 0
              );

              const disponible =
                comprada - devuelta;

              if (disponible <= 0) {
                throw crearError(
                  `${detalle.nombre_producto} ya fue devuelto completamente`,
                  409
                );
              }

              if (cantidadSolicitada > disponible) {
                throw crearError(
                  `Solo quedan ${disponible} unidad(es) de ${detalle.nombre_producto} disponibles para devolución`,
                  409
                );
              }

              detallesDevolucion.push({
                ...detalle,
                cantidad_devolver:
                  cantidadSolicitada
              });
            }
          }

          // =====================================================
          // 2.6 EVITAR DEVOLUCIONES VACÍAS
          // =====================================================

          if (detallesDevolucion.length === 0) {
            throw crearError(
              "Esta compra ya fue devuelta completamente",
              409
            );
          }

          // =====================================================
          // 2.7 CALCULAR IMPORTE
          //
          // Siempre usamos el precio HISTÓRICO de la compra.
          // =====================================================

          let montoDevolucion = 0;

          for (const detalle of detallesDevolucion) {
            const precio = Number(
              detalle.precio_unitario
            );

            montoDevolucion +=
              precio * detalle.cantidad_devolver;
          }

          montoDevolucion = Number(
            montoDevolucion.toFixed(2)
          );

          if (
            !Number.isFinite(montoDevolucion) ||
            montoDevolucion <= 0
          ) {
            throw crearError(
              "El importe de la devolución no es válido",
              500
            );
          }

          const saldoPosterior = Number(
            (saldoAnterior + montoDevolucion)
              .toFixed(2)
          );

          // =====================================================
          // 2.8 CREAR MOVIMIENTO DE DEVOLUCIÓN
          // =====================================================

          const idMovimientoDevolucion =
            randomUUID();

          const concepto = motivo
            ? `Devolución: ${motivo}`
            : "Devolución de compra Genius Bites";

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
              concepto
            )
            VALUES (
              ?, ?, ?, ?, ?,
              'Devolucion',
              ?, ?, ?, ?
            )
            `,
            [
              idMovimientoDevolucion,
              compra.id_cuenta,
              compra.id_alumno,
              compra.id_plantel,
              idUsuario,
              montoDevolucion,
              saldoAnterior,
              idMovimientoCompra,
              concepto.substring(0, 255)
            ]
          );

        // =====================================================
// 2.9 DETALLES DE DEVOLUCIÓN
// =====================================================

for (const detalle of detallesDevolucion) {

  // Crear detalle del nuevo movimiento de devolución
  await conn.execute(
    `
    INSERT INTO monedero_movimiento_detalles (
      id_movimiento_detalle,
      id_movimiento,
      id_movimiento_detalle_origen,
      id_producto_monedero,
      nombre_producto,
      requiere_preparacion,
      cantidad,
      precio_unitario
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      randomUUID(),
      idMovimientoDevolucion,

      // Detalle exacto de la compra original
      detalle.id_movimiento_detalle,

      detalle.id_producto_monedero,
      detalle.nombre_producto,

      Number(detalle.requiere_preparacion) === 1
        ? 1
        : 0,

      detalle.cantidad_devolver,
      Number(detalle.precio_unitario)
    ]
  );

  
  // Actualizar estado del detalle ORIGINAL de compra
const nuevaCantidadDevuelta =
  Number(detalle.cantidad_devuelta || 0) +
  Number(detalle.cantidad_devolver);

const detalleDevuelto =
  nuevaCantidadDevuelta >= Number(detalle.cantidad);

await conn.execute(
  `
  UPDATE monedero_movimiento_detalles
  SET
    cantidad_devuelta = ?,
    devuelto = ?,
    solicitar_devolucion = FALSE,
    cantidad_devolver = NULL
  WHERE id_movimiento_detalle = ?
  `,
  [
    nuevaCantidadDevuelta,
    detalleDevuelto ? 1 : 0,
    detalle.id_movimiento_detalle
  ]
);
}

// =====================================================
// ACTUALIZAR ESTADO DEL MOVIMIENTO ORIGINAL
// =====================================================

const [[estadoCompra]] = await conn.execute(
  `
  SELECT
    COUNT(*) AS pendientes
  FROM monedero_movimiento_detalles
  WHERE id_movimiento = ?
    AND cantidad_devuelta < cantidad
  `,
  [idMovimientoCompra]
);

const compraDevuelta =
  Number(estadoCompra.pendientes) === 0;

await conn.execute(
  `
  UPDATE monedero_movimientos
  SET
    devuelto = ?,
    solicitar_devolucion = FALSE
  WHERE id_movimiento = ?
  `,
  [
    compraDevuelta ? 1 : 0,
    idMovimientoCompra
  ]
);
          // =====================================================
          // 2.10 ACTUALIZAR SALDO
          // =====================================================

          const [actualizacion] = await conn.execute(
            `
            UPDATE monedero_cuentas
            SET saldo_actual = ?
            WHERE id_cuenta = ?
              AND status = 'Activo'
            `,
            [
              saldoPosterior,
              compra.id_cuenta
            ]
          );

          if (actualizacion.affectedRows !== 1) {
            throw crearError(
              "No fue posible actualizar el saldo de la cuenta",
              500
            );
          }

          // =====================================================
          // 2.11 ORDEN DE COCINA
          //
          // No borramos detalles.
          // Calculamos si queda algo pendiente de preparación.
          // =====================================================

          const [[orden]] = await conn.execute(
            `
            SELECT
              id_orden,
              status_orden
            FROM monedero_ordenes
            WHERE id_movimiento_compra = ?
            LIMIT 1
            FOR UPDATE
            `,
            [idMovimientoCompra]
          );

          let ordenCancelada = false;

          if (
            orden &&
            ["Pedido", "Preparado"].includes(
              orden.status_orden
            )
          ) {

            // Ya insertamos la devolución actual,
            // así que esta consulta incluye TODOS los retornos.

            const [[pendientesCocina]] =
              await conn.execute(
                `
                SELECT
                  COALESCE(
                    SUM(
                      GREATEST(
                        compra_det.cantidad
                        -
                        COALESCE(devuelto.cantidad_devuelta, 0),
                        0
                      )
                    ),
                    0
                  ) AS cantidad_pendiente

                FROM monedero_movimiento_detalles
                  compra_det

                LEFT JOIN (
                  SELECT
                    dd.id_movimiento_detalle_origen,
                    SUM(dd.cantidad) AS cantidad_devuelta

                  FROM monedero_movimiento_detalles dd

                  JOIN monedero_movimientos dev
                    ON dev.id_movimiento =
                       dd.id_movimiento

                  WHERE
                    dev.tipo_movimiento = 'Devolucion'
                    AND dev.id_movimiento_origen = ?

                  GROUP BY
                    dd.id_movimiento_detalle_origen

                ) devuelto
                  ON devuelto.id_movimiento_detalle_origen =
                     compra_det.id_movimiento_detalle

                WHERE
                  compra_det.id_movimiento = ?
                  AND compra_det.requiere_preparacion = 1
                `,
                [
                  idMovimientoCompra,
                  idMovimientoCompra
                ]
              );

            const cantidadPendiente = Number(
              pendientesCocina?.cantidad_pendiente || 0
            );

            // Si ya no queda NINGÚN producto que preparar,
            // la orden desaparece del flujo activo de cocina.

            if (cantidadPendiente === 0) {

              await conn.execute(
                `
                UPDATE monedero_ordenes
                SET
                  status_orden = 'Cancelado',
                  fecha_cancelacion = NOW()
                WHERE id_orden = ?
                  AND status_orden IN (
                    'Pedido',
                    'Preparado'
                  )
                `,
                [orden.id_orden]
              );

              ordenCancelada = true;
            }
          }

          // Entregado nunca se modifica retrospectivamente.

          // =====================================================
          // 2.12 LOG
          // =====================================================

          logger.info(
            "Devolución Genius Bites procesada",
            {
              id_movimiento_compra:
                idMovimientoCompra,

              id_movimiento_devolucion:
                idMovimientoDevolucion,

              id_alumno: compra.id_alumno,
              id_cuenta: compra.id_cuenta,
              id_usuario: idUsuario,

              tipo,
              monto: montoDevolucion,

              saldo_anterior: saldoAnterior,
              saldo_posterior: saldoPosterior,

              detalles:
                detallesDevolucion.length,

              id_orden:
                orden?.id_orden || null,

              orden_cancelada:
                ordenCancelada
            }
          );

          // =====================================================
          // RESULTADO
          // =====================================================

          return {
            id_movimiento_devolucion:
              idMovimientoDevolucion,

            id_movimiento_compra:
              idMovimientoCompra,

            tipo,

            monto_devolucion:
              montoDevolucion,

            saldo_anterior:
              saldoAnterior,

            saldo_posterior:
              saldoPosterior,

            id_orden:
              orden?.id_orden || null,

            orden_cancelada:
              ordenCancelada
          };
        }
      );

      // =========================================================
      // 3. RESPUESTA
      // =========================================================

      return res.status(200).json({
        ok: true,
        mensaje: "Devolución realizada correctamente",
        ...resultado,
        duration_ms: Date.now() - startTime
      });

    } catch (error) {

      const mensaje =
        error.message?.startsWith("Monedero:")
          ? error.message
          : "Monedero: No fue posible realizar la devolución";

      logger.error(
        "Error procesando devolución Genius Bites",
        {
          id_movimiento_compra:
            req.body?.id_movimiento_compra || null,

          id_usuario:
          req.usuario?.id_usuario || null,

          error: error.message,
          status_code:
            error.statusCode || 500,

          duration_ms:
            Date.now() - startTime
        }
      );

      return res
        .status(error.statusCode || 500)
        .json({
          ok: false,
          error: mensaje,
          duration_ms:
            Date.now() - startTime
        });
    }
  };
};