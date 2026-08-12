module.exports = function permisosMonederoFactory({
  logger
}) {

  const permisosPorRol = {
    directivo: [
      "caja",
      "cocina"
    ],

    administrativo: [
      "caja",
      "cocina"
    ],

    coordinador: [
      "caja",
      "cocina"
    ],

    auxiliar: [
      "cocina"
    ],

    maestro: []
  };

  function obtenerPermisos(rol) {
    return (
      permisosPorRol[
        String(rol || "")
          .trim()
          .toLowerCase()
      ] || []
    );
  }

  function requireMonedero(req, res, next) {

    if (!req.usuario) {
      return res.status(401).json({
        ok: false,
        error: "Sesión requerida"
      });
    }

    const permisos =
      obtenerPermisos(req.usuario.rol);

    if (permisos.length === 0) {
      return res.status(403).json({
        ok: false,
        error:
          "No tienes acceso a Genius Bites"
      });
    }

    req.monedero = {
      ...req.usuario,
      permisos
    };

    /*
     * Compatibilidad temporal con los módulos actuales.
     * procesar-compra, devolución y consultar-datos
     * todavía leen req.caja.
     *
     * Así NO tenemos que modificarlos hoy.
     */
    req.caja = req.monedero;

    next();
  }

  function requirePermiso(permiso) {

    return function (req, res, next) {

      if (!req.usuario) {
        return res.status(401).json({
          ok: false,
          error: "Sesión requerida"
        });
      }

      const permisos =
        obtenerPermisos(req.usuario.rol);

      if (!permisos.includes(permiso)) {

        logger.warn(
          "Acceso Genius Bites rechazado",
          {
            id_usuario:
              req.usuario.id_usuario,
            rol:
              req.usuario.rol,
            permiso
          }
        );

        return res.status(403).json({
          ok: false,
          error:
            "No tienes permiso para esta sección"
        });
      }

      req.monedero = {
        ...req.usuario,
        permisos
      };

      // Compatibilidad actual
      req.caja = req.monedero;

      next();
    };
  }

  function menuHandler(req, res) {

    const permisos =
      obtenerPermisos(req.usuario.rol);

    return res.json({
      ok: true,

      usuario: req.usuario,

      modulos: {
        caja:
          permisos.includes("caja"),

        cocina:
          permisos.includes("cocina")
      }
    });
  }

  return {
    requireMonedero,

    requireCaja:
      requirePermiso("caja"),

    requireCocina:
      requirePermiso("cocina"),

    menuHandler,

    obtenerPermisos
  };
};