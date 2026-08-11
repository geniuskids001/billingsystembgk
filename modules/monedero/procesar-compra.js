const { randomUUID } = require("crypto");

module.exports = function procesarCompraFactory({
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

  // Helpers de dinero: todo se calcula en centavos (enteros)
  // para evitar acumulación de errores de punto flotante.
  function pesosACentavos(pesos) {
    return Math.round(Number(pesos) * 100);
  }

  function centavosAPesos(centavos) {
    return centavos / 100;
  }

  return async function procesarCompraHandler(req, res) {
    const startTime = Date.now();

    try {
      const idAlumno = String(
        req.body?.id_alumno || ""
      ).trim();

      const productos = req.body?.productos;

      const observaciones = String(
        req.body?.observaciones || ""
      ).trim() || null;

      // Usuario REAL obtenido del access_token de Caja
      const idUsuario = req.caja?.id_usuario;
      const idPlantelCaja = req.caja?.id_plantel || null;

      // =========================================================
      // 1. VALIDAR ENTRADA
      // =========================================================

      if (!idAlumno) {
        throw crearError(
          "Selecciona un alumno",
          400
        );
      }

      if (!idUsuario) {
        throw crearError(
          "No fue posible identificar al usuario de caja",
          401
        );
      }

      if (!Array.isArray(productos) || productos.length === 0) {
        throw crearError(
          "Agrega al menos un producto",
          400
        );
      }

      if (productos.length > 50) {
        throw crearError(
          "Demasiados productos en la compra",
          400
        );
      }

      if (observaciones && observaciones.length > 255) {
        throw crearError(
          "Las observaciones son demasiado largas",
          400
        );
      }

      // Validar estructura y agrupar productos repetidos
      const productosAgrupados = new Map();

      for (const item of productos) {
        const idProducto = String(
          item?.id_producto_monedero || ""
        ).trim();

        const cantidad = Number(item?.cantidad);

        if (!idProducto) {
          throw crearError(
            "Uno de los productos es inválido",
            400
          );
        }

        if (
          !Number.isInteger(cantidad) ||
          cantidad <= 0
        ) {
          throw crearError(
            "La cantidad de uno de los productos es inválida",
            400
          );
        }

        productosAgrupados.set(
          idProducto,
          (productosAgrupados.get(idProducto) || 0) + cantidad
        );
      }

      // =========================================================
      // 2. TRANSACCIÓN
      // =========================================================

      const resultado = await executeInTransaction(async (conn) => {

        // =======================================================
        // 2.1 ALUMNO
        // =======================================================

        const [[alumno]] = await conn.execute(
          `
          SELECT
            id_alumno,
            id_plantel_academico,
            status
          FROM alumnos
          WHERE id_alumno = ?
          LIMIT 1
          `,
          [idAlumno]
        );

        if (!alumno) {
          throw crearError(
            "El alumno no existe",
            404
          );
        }

        if (alumno.status !== "Activo") {
          throw crearError(
            "El alumno está inactivo",
            409
          );
        }

        if (!alumno.id_plantel_academico) {
          throw crearError(
            "El alumno no tiene plantel académico",
            409
          );
        }

        const idPlantel = alumno.id_plantel_academico;

        // Si el cajero pertenece a un plantel, solo opera ahí.
        // Directivos con id_plantel NULL no se bloquean.
        if (
          idPlantelCaja &&
          idPlantelCaja !== idPlantel
        ) {
          throw crearError(
            "No tienes autorización para cobrar a alumnos de este plantel",
            403
          );
        }

        // =======================================================
        // 2.2 CONFIGURACIÓN GENIUS BITES DEL PLANTEL
        // =======================================================

        const [[configuracion]] = await conn.execute(
          `
          SELECT id_configuracion
          FROM monedero_configuracion_plantel
          WHERE id_plantel = ?
            AND status = 'Activo'
          LIMIT 1
          `,
          [idPlantel]
        );

        if (!configuracion) {
          throw crearError(
            "Genius Bites no está activo para este plantel",
            409
          );
        }

        // =======================================================
        // 2.3 CUENTA + RELACIÓN
        //
        // FOR UPDATE serializa operaciones sobre esta cuenta.
        // =======================================================

        const [[cuenta]] = await conn.execute(
          `
          SELECT
            mca.id_cuenta_alumno,
            mca.id_cuenta,
            mca.limite_gasto_diario,
            mca.status AS status_relacion,

            mc.saldo_actual,
            mc.limite_negativo,
            mc.status AS status_cuenta

          FROM monedero_cuenta_alumnos mca

          JOIN monedero_cuentas mc
            ON mc.id_cuenta = mca.id_cuenta

          WHERE mca.id_alumno = ?

          LIMIT 1
          FOR UPDATE
          `,
          [idAlumno]
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

        // Todo el manejo de saldo se hace en centavos (enteros)
        // para evitar errores de precisión de punto flotante.
        const saldoAnteriorCentavos = pesosACentavos(
          cuenta.saldo_actual
        );

        const limiteNegativoCentavos = pesosACentavos(
          cuenta.limite_negativo
        );

        const limiteDiarioCentavos =
          cuenta.limite_gasto_diario === null
            ? null
            : pesosACentavos(cuenta.limite_gasto_diario);

        if (
          !Number.isFinite(saldoAnteriorCentavos) ||
          !Number.isFinite(limiteNegativoCentavos)
        ) {
          throw crearError(
            "La cuenta tiene una configuración inválida",
            500
          );
        }

        // =======================================================
        // 2.4 PRODUCTOS
        // =======================================================

        const idsProductos = [
          ...productosAgrupados.keys()
        ];

        const placeholders = idsProductos
          .map(() => "?")
          .join(",");

        const [productosDb] = await conn.execute(
          `
          SELECT
            id_producto_monedero,
            id_plantel,
            nombre,
            precio,
            requiere_preparacion,
            status
          FROM monedero_productos
          WHERE id_producto_monedero IN (${placeholders})
          `,
          idsProductos
        );

        if (productosDb.length !== idsProductos.length) {
          throw crearError(
            "Uno o más productos ya no existen",
            409
          );
        }

        let totalCompraCentavos = 0;
        let requiereCocina = false;

        const detalles = [];

        for (const producto of productosDb) {

          if (producto.status !== "Activo") {
            throw crearError(
              `${producto.nombre} ya no está disponible`,
              409
            );
          }

          if (producto.id_plantel !== idPlantel) {
            throw crearError(
              `${producto.nombre} no pertenece a este plantel`,
              409
            );
          }

          const precioCentavos = pesosACentavos(producto.precio);

          if (
            !Number.isFinite(precioCentavos) ||
            precioCentavos < 0
          ) {
            throw crearError(
              `${producto.nombre} tiene un precio inválido`,
              500
            );
          }

          const cantidad = productosAgrupados.get(
            producto.id_producto_monedero
          );

          totalCompraCentavos += precioCentavos * cantidad;

          const requierePreparacion =
            Number(producto.requiere_preparacion) === 1;

          if (requierePreparacion) {
            requiereCocina = true;
          }

          detalles.push({
            id_producto_monedero:
              producto.id_producto_monedero,
            nombre: producto.nombre,
            precio: centavosAPesos(precioCentavos),
            cantidad,
            requiere_preparacion:
              requierePreparacion
          });
        }

        if (totalCompraCentavos <= 0) {
          throw crearError(
            "El total de la compra debe ser mayor a cero",
            400
          );
        }

        // =======================================================
        // 2.5 LÍMITE DIARIO
        //
        // Compras de HOY menos devoluciones relacionadas
        // con compras de HOY.
        // =======================================================

        let gastoHoyCentavos = 0;

        if (limiteDiarioCentavos !== null) {

          const [[gasto]] = await conn.execute(
            `
            SELECT
              COALESCE(SUM(valor), 0) AS gasto_hoy
            FROM (
              
              SELECT
                -m.monto AS valor
              FROM monedero_movimientos m
              WHERE m.id_alumno = ?
                AND m.tipo_movimiento = 'Compra'
                AND m.fecha_movimiento >= CURDATE()
                AND m.fecha_movimiento < CURDATE() + INTERVAL 1 DAY

              UNION ALL

              SELECT
                -d.monto AS valor
              FROM monedero_movimientos d

              JOIN monedero_movimientos compra
                ON compra.id_movimiento =
                   d.id_movimiento_origen

              WHERE d.id_alumno = ?
                AND d.tipo_movimiento = 'Devolucion'
                AND compra.tipo_movimiento = 'Compra'
                AND compra.fecha_movimiento >= CURDATE()
                AND compra.fecha_movimiento <
                    CURDATE() + INTERVAL 1 DAY

            ) movimientos_dia
            `,
            [idAlumno, idAlumno]
          );

          gastoHoyCentavos = pesosACentavos(
            gasto?.gasto_hoy || 0
          );

          const gastoPosteriorCentavos =
            gastoHoyCentavos + totalCompraCentavos;

          if (gastoPosteriorCentavos > limiteDiarioCentavos) {

            const disponibleCentavos = Math.max(
              0,
              limiteDiarioCentavos - gastoHoyCentavos
            );

            throw crearError(
              `La compra supera el límite diario. Disponible hoy: $${centavosAPesos(disponibleCentavos).toFixed(2)}`,
              409
            );
          }
        }

        // =======================================================
        // 2.6 SALDO + LÍMITE DE DEUDA
        // =======================================================

        const saldoPosteriorCentavos =
          saldoAnteriorCentavos - totalCompraCentavos;

        if (saldoPosteriorCentavos < -limiteNegativoCentavos) {

          const disponibleCentavos = Math.max(
            0,
            saldoAnteriorCentavos + limiteNegativoCentavos
          );

          throw crearError(
            `Saldo insuficiente. Disponible incluyendo límite de deuda: $${centavosAPesos(disponibleCentavos).toFixed(2)}`,
            409
          );
        }

        const totalCompra = centavosAPesos(totalCompraCentavos);
        const saldoAnterior = centavosAPesos(saldoAnteriorCentavos);
        const saldoPosterior = centavosAPesos(saldoPosteriorCentavos);

        // =======================================================
        // 2.7 CREAR MOVIMIENTO
        // =======================================================

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
            concepto
          )
          VALUES (
            ?, ?, ?, ?, ?,
            'Compra',
            ?, ?,
            'Compra Genius Bites'
          )
          `,
          [
            idMovimiento,
            cuenta.id_cuenta,
            idAlumno,
            idPlantel,
            idUsuario,
            -totalCompra,
            saldoAnterior
          ]
        );

        // =======================================================
        // 2.8 DETALLES (batch insert — una sola query)
        // =======================================================

        const detallesPlaceholders = detalles
          .map(() => "(?, ?, ?, ?, ?, ?, ?)")
          .join(", ");

        const detallesValores = detalles.flatMap((detalle) => [
          randomUUID(),
          idMovimiento,
          detalle.id_producto_monedero,
          detalle.nombre,
          detalle.requiere_preparacion ? 1 : 0,
          detalle.cantidad,
          detalle.precio
        ]);

        await conn.execute(
          `
          INSERT INTO monedero_movimiento_detalles (
            id_movimiento_detalle,
            id_movimiento,
            id_producto_monedero,
            nombre_producto,
            requiere_preparacion,
            cantidad,
            precio_unitario
          )
          VALUES ${detallesPlaceholders}
          `,
          detallesValores
        );

        // =======================================================
        // 2.9 ACTUALIZAR SALDO MATERIALIZADO
        // =======================================================

        const [actualizacion] = await conn.execute(
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

        if (actualizacion.affectedRows !== 1) {
          throw crearError(
            "No fue posible actualizar el saldo",
            500
          );
        }

        // =======================================================
        // 2.10 ORDEN DE COCINA
        // =======================================================

        let idOrden = null;

        if (requiereCocina) {

          idOrden = randomUUID();

          await conn.execute(
            `
            INSERT INTO monedero_ordenes (
              id_orden,
              id_movimiento_compra,
              status_orden,
              observaciones
            )
            VALUES (?, ?, 'Pedido', ?)
            `,
            [
              idOrden,
              idMovimiento,
              observaciones
            ]
          );
        }

        // =======================================================
        // RESULTADO
        // =======================================================

        logger.info("Compra Genius Bites procesada", {
          id_movimiento: idMovimiento,
          id_orden: idOrden,
          id_alumno: idAlumno,
          id_cuenta: cuenta.id_cuenta,
          id_usuario: idUsuario,
          total: totalCompra,
          saldo_anterior: saldoAnterior,
          saldo_posterior: saldoPosterior,
          requiere_cocina: requiereCocina
        });

        return {
          id_movimiento: idMovimiento,
          id_orden: idOrden,
          total: totalCompra,
          saldo_anterior: saldoAnterior,
          saldo_posterior: saldoPosterior,
          requiere_cocina: requiereCocina
        };
      });

      // =========================================================
      // 3. RESPUESTA
      // =========================================================

      return res.status(200).json({
        ok: true,
        mensaje: "Compra realizada correctamente",
        ...resultado,
        duration_ms: Date.now() - startTime
      });

    } catch (error) {

      const mensaje =
        error.message?.startsWith("Monedero:")
          ? error.message
          : "Monedero: No fue posible realizar la compra";

      logger.error("Error procesando compra Genius Bites", {
        id_alumno: req.body?.id_alumno || null,
        id_usuario: req.caja?.id_usuario || null,
        error: error.message,
        status_code: error.statusCode || 500,
        duration_ms: Date.now() - startTime
      });

      return res
        .status(error.statusCode || 500)
        .json({
          ok: false,
          error: mensaje,
          duration_ms: Date.now() - startTime
        });
    }
  };
};