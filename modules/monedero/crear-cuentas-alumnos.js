const { randomUUID, randomInt } = require("crypto");

/**
 * Crea una cuenta de monedero para uno o varios alumnos.
 *
 * Payload aceptado:
 *
 * Un alumno:
 * {
 *   "id_alumno": "uuid-del-alumno"
 * }
 *
 * Varios alumnos:
 * {
 *   "ids_alumnos": [
 *     "uuid-alumno-1",
 *     "uuid-alumno-2"
 *   ]
 * }
 */
module.exports = function crearCuentasAlumnosFactory({
  pool,
  executeInTransaction,
  logger
}) {
  /**
   * Genera un número de cuenta legible.
   *
   * Ejemplo:
   * A 14 12 34
   *
   * Formato:
   * - Una letra
   * - Seis dígitos divididos en tres grupos
   */
  function generarNumeroCuenta() {
    const letras = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const letra = letras[randomInt(0, letras.length)];

    const numero = randomInt(0, 1_000_000)
      .toString()
      .padStart(6, "0");

    return `${letra} ${numero.slice(0, 2)} ${numero.slice(2, 4)} ${numero.slice(4, 6)}`;
  }

  /**
   * Intenta generar un número de cuenta que no exista.
   *
   * Aunque después se conserva la restricción UNIQUE de la BD,
   * esta validación reduce considerablemente las colisiones.
   */
  async function generarNumeroCuentaDisponible(conn) {
    const maxIntentos = 20;

    for (let intento = 1; intento <= maxIntentos; intento += 1) {
      const numCuenta = generarNumeroCuenta();

      const [[cuentaExistente]] = await conn.execute(
        `
        SELECT id_cuenta
        FROM monedero_cuentas
        WHERE num_cuenta = ?
        LIMIT 1
        `,
        [numCuenta]
      );

      if (!cuentaExistente) {
        return numCuenta;
      }
    }

    const error = new Error(
      "No fue posible generar un número de cuenta disponible"
    );
    error.statusCode = 500;
    throw error;
  }

  /**
   * Crea la cuenta de un solo alumno.
   *
   * Cada alumno se procesa en su propia transacción para que,
   * en una solicitud en lote, el error de un alumno no revierta
   * las cuentas creadas correctamente para los demás.
   */
  async function crearCuentaAlumno(idAlumno) {
    return executeInTransaction(async (conn) => {
      // ============================================================
      // 1. Obtener y bloquear el alumno
      // ============================================================
      const [[alumno]] = await conn.execute(
        `
        SELECT
          id_alumno,
          id_plantel_academico,
          nombre,
          apellido_paterno,
          apellido_materno,
          status
        FROM alumnos
        WHERE id_alumno = ?
        FOR UPDATE
        `,
        [idAlumno]
      );

      if (!alumno) {
        const error = new Error("Alumno no encontrado");
        error.statusCode = 404;
        throw error;
      }

      if (!alumno.id_plantel_academico) {
        const error = new Error(
          "El alumno no tiene un plantel académico asignado"
        );
        error.statusCode = 400;
        throw error;
      }

      // ============================================================
      // 2. Verificar si el alumno ya tiene cuenta
      // ============================================================
      const [[relacionExistente]] = await conn.execute(
        `
        SELECT
          mca.id_cuenta_alumno,
          mca.id_cuenta,
          mca.id_alumno,
          mca.status AS status_relacion,
          mc.num_cuenta,
          mc.saldo_actual,
          mc.limite_negativo,
          mc.status AS status_cuenta
        FROM monedero_cuenta_alumnos mca
        JOIN monedero_cuentas mc
          ON mc.id_cuenta = mca.id_cuenta
        WHERE mca.id_alumno = ?
        LIMIT 1
        `,
        [idAlumno]
      );

      if (relacionExistente) {
        return {
          status: "existente",
          creado: false,
          id_alumno: idAlumno,
          id_cuenta: relacionExistente.id_cuenta,
          num_cuenta: relacionExistente.num_cuenta,
          saldo_actual: Number(relacionExistente.saldo_actual),
          limite_negativo: Number(relacionExistente.limite_negativo),
          mensaje: "Este alumno ya tiene una cuenta asignada"
        };
      }

      // ============================================================
      // 3. Obtener configuración activa del plantel
      // ============================================================
      const [[configuracion]] = await conn.execute(
        `
        SELECT
          id_plantel,
          limite_negativo_default,
          status
        FROM monedero_configuracion_plantel
        WHERE id_plantel = ?
          AND status = 'Activo'
        LIMIT 1
        `,
        [alumno.id_plantel_academico]
      );

      if (!configuracion) {
        const error = new Error(
          "El plantel del alumno no tiene una configuración activa de monedero"
        );
        error.statusCode = 409;
        throw error;
      }

      const limiteNegativo = Number(
        configuracion.limite_negativo_default
      );

      if (
        !Number.isFinite(limiteNegativo) ||
        limiteNegativo < 0
      ) {
        const error = new Error(
          "La configuración del plantel tiene un límite negativo inválido"
        );
        error.statusCode = 500;
        throw error;
      }

      // ============================================================
      // 4. Generar identificadores
      // ============================================================
      const idCuenta = randomUUID();
      const idCuentaAlumno = randomUUID();

      /*
       * La tabla actual exige qr_token NOT NULL y UNIQUE.
       * Aunque el QR todavía no se utilice en el frontend,
       * generamos el token desde ahora para respetar el esquema.
       */
      const qrToken = randomUUID();

      const numCuenta = await generarNumeroCuentaDisponible(conn);

      // ============================================================
      // 5. Crear cuenta
      // ============================================================
      await conn.execute(
        `
        INSERT INTO monedero_cuentas (
          id_cuenta,
          num_cuenta,
          saldo_actual,
          limite_negativo,
          status
        )
        VALUES (?, ?, 0.00, ?, 'Activo')
        `,
        [
          idCuenta,
          numCuenta,
          limiteNegativo
        ]
      );

      // ============================================================
      // 6. Vincular alumno con la cuenta
      // ============================================================
      await conn.execute(
        `
        INSERT INTO monedero_cuenta_alumnos (
          id_cuenta_alumno,
          id_cuenta,
          id_alumno,
          limite_gasto_diario,
          qr_token,
          status
        )
        VALUES (?, ?, ?, NULL, ?, 'Activo')
        `,
        [
          idCuentaAlumno,
          idCuenta,
          idAlumno,
          qrToken
        ]
      );

      logger.info("Cuenta de monedero creada para alumno", {
        id_alumno: idAlumno,
        id_plantel: alumno.id_plantel_academico,
        id_cuenta: idCuenta,
        num_cuenta: numCuenta,
        limite_negativo: limiteNegativo
      });

      return {
        status: "creada",
        creado: true,
        id_alumno: idAlumno,
        id_plantel: alumno.id_plantel_academico,
        id_cuenta: idCuenta,
        id_cuenta_alumno: idCuentaAlumno,
        num_cuenta: numCuenta,
        saldo_actual: 0,
        limite_negativo: limiteNegativo,
        limite_gasto_diario: null,
        mensaje: "Cuenta creada correctamente"
      };
    });
  }

  return async function crearCuentasAlumnosHandler(req, res, next) {
    const startTime = Date.now();

    try {
      const { id_alumno, ids_alumnos } = req.body || {};

      // ============================================================
      // 1. Normalizar entrada
      // ============================================================
      let idsAlumnos = [];

      if (id_alumno !== undefined && id_alumno !== null) {
        idsAlumnos.push(String(id_alumno).trim());
      }

      if (Array.isArray(ids_alumnos)) {
  idsAlumnos.push(
    ...ids_alumnos.map((id) => String(id || "").trim())
  );
} else if (typeof ids_alumnos === "string") {
  idsAlumnos.push(
    ...ids_alumnos
      .split(",")
      .map((id) => id.trim())
      .filter(Boolean)
  );
}

      // Quitar valores vacíos y duplicados
      idsAlumnos = [
        ...new Set(idsAlumnos.filter(Boolean))
      ];

      if (idsAlumnos.length === 0) {
        return res.status(400).json({
          ok: false,
          error: "Debes enviar id_alumno o ids_alumnos"
        });
      }

      /*
       * Límite defensivo para evitar solicitudes excesivamente grandes.
       * Puede aumentarse posteriormente si se requiere una migración masiva.
       */
      const limiteLote = 200;

      if (idsAlumnos.length > limiteLote) {
        return res.status(400).json({
          ok: false,
          error: `Solo se permiten hasta ${limiteLote} alumnos por solicitud`
        });
      }

      logger.info("Iniciando creación de cuentas de monedero", {
        cantidad_alumnos: idsAlumnos.length
      });

      // ============================================================
      // 2. Procesar alumnos
      // ============================================================
      const resultados = [];

      /*
       * Se procesan secuencialmente para no saturar el pool de MySQL
       * ni abrir demasiadas transacciones simultáneas.
       */
      for (const idAlumno of idsAlumnos) {
        try {
          const resultado = await crearCuentaAlumno(idAlumno);
          resultados.push(resultado);
        } catch (error) {
          /*
           * Si dos solicitudes intentan registrar al mismo alumno
           * simultáneamente, la restricción UNIQUE de id_alumno
           * protege la integridad.
           */
          if (
            error.code === "ER_DUP_ENTRY" &&
            String(error.message).includes("uq_monedero_alumno")
          ) {
            const [[cuentaExistente]] = await pool.execute(
              `
              SELECT
                mca.id_cuenta,
                mc.num_cuenta,
                mc.saldo_actual,
                mc.limite_negativo
              FROM monedero_cuenta_alumnos mca
              JOIN monedero_cuentas mc
                ON mc.id_cuenta = mca.id_cuenta
              WHERE mca.id_alumno = ?
              LIMIT 1
              `,
              [idAlumno]
            );

            resultados.push({
              status: "existente",
              creado: false,
              id_alumno: idAlumno,
              id_cuenta: cuentaExistente?.id_cuenta || null,
              num_cuenta: cuentaExistente?.num_cuenta || null,
              saldo_actual:
                cuentaExistente?.saldo_actual !== undefined
                  ? Number(cuentaExistente.saldo_actual)
                  : null,
              limite_negativo:
                cuentaExistente?.limite_negativo !== undefined
                  ? Number(cuentaExistente.limite_negativo)
                  : null,
              mensaje: "Este alumno ya tiene una cuenta asignada"
            });

            continue;
          }

          logger.error(
            "No se pudo crear cuenta de monedero para alumno",
            {
              id_alumno: idAlumno,
              error: error.message,
              error_code: error.code
            }
          );

          resultados.push({
            status: "error",
            creado: false,
            id_alumno: idAlumno,
            error: error.message,
            status_code: error.statusCode || 500
          });
        }
      }

      // ============================================================
      // 3. Construir resumen
      // ============================================================
      const creadas = resultados.filter(
        (resultado) => resultado.status === "creada"
      ).length;

      const existentes = resultados.filter(
        (resultado) => resultado.status === "existente"
      ).length;

      const errores = resultados.filter(
        (resultado) => resultado.status === "error"
      ).length;

      const duration = Date.now() - startTime;

      logger.info("Creación de cuentas de monedero terminada", {
        solicitadas: idsAlumnos.length,
        creadas,
        existentes,
        errores,
        duration_ms: duration
      });

      /*
       * La petición HTTP fue correctamente procesada aunque algún alumno
       * individual haya tenido error. Por eso la respuesta general es 200
       * y los errores individuales aparecen en resultados.
       */
      return res.status(200).json({
        ok: errores === 0,
        procesado: true,
        resumen: {
          solicitadas: idsAlumnos.length,
          creadas,
          existentes,
          errores
        },
        resultados,
        duration_ms: duration,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      next(error);
    }
  };
};