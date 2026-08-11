module.exports = function consultarDatosFactory({
  pool,
  logger
}) {

  function crearError(message, statusCode = 400) {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
  }

  return async function consultarDatosHandler(req, res, next) {
    try {
      const recurso = String(req.params.recurso || "")
        .trim()
        .toLowerCase();

      const usuario = req.caja;

      if (!usuario) {
        throw crearError("Sesión de Caja requerida", 401);
      }

      const rol = String(usuario.rol || "")
        .trim()
        .toLowerCase();

      // =========================================================
      // RECURSOS PERMITIDOS
      // =========================================================
      const recursos = {
  alumnos: {
    vista: "vw_monedero_alumnos_cuentas",
    orden: "nombre_completo ASC"
  },

  productos: {
    vista: "vw_monedero_productos_catalogo",
    orden: "categoria ASC, nombre ASC"
  },

  movimientos: {
    vista: "vw_monedero_movimientos_recientes",
    orden: "fecha_movimiento DESC, id_movimiento DESC"
  },

  usuarios: {
    vista: "vw_monedero_usuarios_acceso",
    orden: "nombre_completo ASC"
  }
};

      const config = recursos[recurso];

      if (!config) {
        throw crearError("Recurso no válido", 404);
      }

      // =========================================================
      // QUERY BASE
      // =========================================================
      let sql = `
        SELECT *
        FROM ${config.vista}
      `;

      const where = [];
      const params = [];

      // =========================================================
      // PERMISOS
      // =========================================================

      // DIRECTIVO → TODO
      if (rol === "directivo") {
        // Sin filtro adicional
      }

      // MAESTRO → SOLO SUS ALUMNOS
      else if (rol === "maestro") {

        if (recurso === "alumnos") {
          where.push("id_maestro = ?");
          params.push(usuario.id_usuario);
        }

        else if (recurso === "movimientos") {
          where.push(`
            id_alumno IN (
              SELECT id_alumno
              FROM vw_monedero_alumnos_cuentas
              WHERE id_maestro = ?
            )
          `);

          params.push(usuario.id_usuario);
        }

        else if (recurso === "productos") {
          if (!usuario.id_plantel) {
            throw crearError(
              "El maestro no tiene plantel asignado",
              403
            );
          }

          where.push("id_plantel = ?");
          params.push(usuario.id_plantel);
        }
      }

      // PERSONAL DE PLANTEL
      else if (
        ["coordinador", "administrativo", "auxiliar"].includes(rol)
      ) {
        if (!usuario.id_plantel) {
          throw crearError(
            "El usuario no tiene plantel asignado",
            403
          );
        }

        where.push("id_plantel = ?");
        params.push(usuario.id_plantel);
      }

      // CUALQUIER OTRO ROL
      else {
        throw crearError(
          "El usuario no tiene permisos para Genius Bites",
          403
        );
      }

      // =========================================================
      // FILTROS OPCIONALES SEGUROS
      // =========================================================

      if (recurso === "movimientos" && req.query.id_alumno) {
        where.push("id_alumno = ?");
        params.push(String(req.query.id_alumno).trim());
      }

      if (recurso === "productos" && req.query.categoria) {
        where.push("categoria = ?");
        params.push(String(req.query.categoria).trim());
      }

      if (where.length > 0) {
        sql += ` WHERE ${where.join(" AND ")}`;
      }

      sql += ` ORDER BY ${config.orden}`;

      const [rows] = await pool.execute(sql, params);

      logger.info("Consulta Genius Bites completada", {
        evento: "MONEDERO_DATOS_CONSULTADOS",
        recurso,
        id_usuario: usuario.id_usuario,
        rol: usuario.rol,
        id_plantel: usuario.id_plantel,
        registros: rows.length
      });

      return res.json({
        ok: true,
        recurso,
        data: rows
      });

    } catch (error) {
      next(error);
    }
  };
};