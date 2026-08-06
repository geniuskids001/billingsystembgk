async function cancelarCargosMensualesCore(
  conn,
  mes,
  anio,
  alumnosList = null,
  motivo = "Cancelación masiva"
) {
  // ============================================================
  // 1. Resolver periodo
  // ============================================================
  if (mes == null || anio == null) {
    const [[fechaActual]] = await conn.execute(`
      SELECT
        MONTH(CURDATE()) AS mes,
        YEAR(CURDATE()) AS anio
    `);

    mes = mes ?? fechaActual.mes;
    anio = anio ?? fechaActual.anio;
  }

  mes = Number(mes);
  anio = Number(anio);

  if (!Number.isInteger(mes) || mes < 1 || mes > 12) {
    const error = new Error("Mes inválido");
    error.statusCode = 400;
    throw error;
  }

  if (!Number.isInteger(anio) || anio < 2000 || anio > 2100) {
    const error = new Error("Año inválido");
    error.statusCode = 400;
    throw error;
  }

  motivo = String(
    motivo || "Cancelación masiva"
  ).trim().substring(0, 255);

  // ============================================================
  // 2. Filtro opcional de alumnos
  // ============================================================
  const filtroAlumnos =
    alumnosList && alumnosList.length > 0
      ? `AND ac.id_alumno IN (${alumnosList.map(() => "?").join(",")})`
      : "";

  const params = [
    motivo,
    mes,
    anio,
    ...(alumnosList && alumnosList.length > 0
      ? alumnosList
      : [])
  ];

  // ============================================================
  // 3. Cancelar únicamente cargos no pagados
  // ============================================================
  const [result] = await conn.execute(
    `
    UPDATE alumnos_cargos ac
    SET
      ac.status_cargo = 'Cancelado',
      ac.motivo_cancelacion = ?,
      ac.solicita_cancelacion = 0,
      ac.updated_at = NOW()
    WHERE ac.mes = ?
      AND ac.anio = ?
      AND ac.status_cargo = 'Activo'
      ${filtroAlumnos}
      AND NOT EXISTS (
        SELECT 1
        FROM recibos r
        JOIN recibos_detalle rd
          ON rd.id_recibo = r.id_recibo
        WHERE r.id_alumno = ac.id_alumno
          AND r.status_recibo = 'Emitido'
          AND rd.id_producto = ac.id_producto
          AND rd.mes = ac.mes
          AND rd.anio = ac.anio
      )
    `,
    params
  );

  return {
    mes,
    anio,
    alumnos_filtrados:
      alumnosList && alumnosList.length > 0
        ? alumnosList.length
        : null,
    cancelados: result.affectedRows,
    motivo
  };
}

function cancelarCargosMensualesFactory({
  executeInTransaction,
  logger
}) {
  return async function cancelarCargosMensualesHandler(
    req,
    res,
    next
  ) {
    const startTime = Date.now();

    let {
      mes,
      anio,
      alumnos,
      ids_alumnos,
      motivo
    } = req.body || {};

    try {
      // Acepta:
      // alumnos: "id1,id2"
      // alumnos: ["id1", "id2"]
      // ids_alumnos: ["id1", "id2"]

      const entradaAlumnos =
        ids_alumnos !== undefined
          ? ids_alumnos
          : alumnos;

      let alumnosList = null;

      if (Array.isArray(entradaAlumnos)) {
        alumnosList = entradaAlumnos
          .map((id) => String(id || "").trim())
          .filter(Boolean);
      } else if (
        entradaAlumnos !== undefined &&
        entradaAlumnos !== null &&
        String(entradaAlumnos).trim() !== ""
      ) {
        alumnosList = String(entradaAlumnos)
          .split(",")
          .map((id) => id.trim())
          .filter(Boolean);
      }

      if (alumnosList) {
        alumnosList = [...new Set(alumnosList)];
      }

      logger.info("Iniciando cancelación mensual de cargos", {
        mes,
        anio,
        cantidad_alumnos: alumnosList?.length || "todos",
        motivo
      });

      const result = await executeInTransaction(
        async (conn) => {
          return cancelarCargosMensualesCore(
            conn,
            mes,
            anio,
            alumnosList,
            motivo
          );
        }
      );

      const duration = Date.now() - startTime;

      logger.info("Cancelación mensual terminada", {
        ...result,
        duration_ms: duration
      });

      return res.json({
        ok: true,
        ...result,
        duration_ms: duration
      });

    } catch (error) {
      logger.error(
        "Error al cancelar cargos mensuales",
        {
          mes,
          anio,
          alumnos,
          ids_alumnos,
          motivo,
          error: error.message,
          error_code: error.code,
          duration_ms: Date.now() - startTime
        }
      );

      next(error);
    }
  };
}

module.exports = cancelarCargosMensualesFactory;
module.exports.core = cancelarCargosMensualesCore;