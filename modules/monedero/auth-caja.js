const crypto = require("crypto");

module.exports = function authCajaFactory({
  pool,
  logger,
  secret,
  sendAccessCode
}) {
  const CHALLENGE_SECONDS = 10 * 60;
  const SESSION_SECONDS = 12 * 60 * 60;
  const LAUNCH_SECONDS = 5 * 60;

  if (!secret) {
    throw new Error("MONEDERO_AUTH_SECRET es requerido");
  }

  // Control básico en memoria. No sustituye una tabla distribuida,
  // pero evita solicitudes repetidas accidentales.
  const enviosRecientes = new Map();
  const intentosCodigo = new Map();

  function crearError(message, statusCode = 400) {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
  }

  function base64Url(value) {
    return Buffer.from(value).toString("base64url");
  }

  function firmar(payload) {
    const header = {
      alg: "HS256",
      typ: "GB-AUTH"
    };

    const headerEncoded = base64Url(JSON.stringify(header));
    const payloadEncoded = base64Url(JSON.stringify(payload));
    const unsignedToken = `${headerEncoded}.${payloadEncoded}`;

    const signature = crypto
      .createHmac("sha256", secret)
      .update(unsignedToken)
      .digest("base64url");

    return `${unsignedToken}.${signature}`;
  }

  function verificarFirma(token) {
    const partes = String(token || "").split(".");

    if (partes.length !== 3) {
      throw crearError("Token inválido", 401);
    }

    const [headerEncoded, payloadEncoded, signature] = partes;
    const unsignedToken = `${headerEncoded}.${payloadEncoded}`;

    const expectedSignature = crypto
      .createHmac("sha256", secret)
      .update(unsignedToken)
      .digest("base64url");

    const signatureBuffer = Buffer.from(signature);
    const expectedBuffer = Buffer.from(expectedSignature);

    if (
      signatureBuffer.length !== expectedBuffer.length ||
      !crypto.timingSafeEqual(signatureBuffer, expectedBuffer)
    ) {
      throw crearError("Token inválido", 401);
    }

    let payload;

    try {
      payload = JSON.parse(
        Buffer.from(payloadEncoded, "base64url").toString("utf8")
      );
    } catch {
      throw crearError("Token inválido", 401);
    }

    const now = Math.floor(Date.now() / 1000);

    if (!payload.exp || payload.exp <= now) {
      throw crearError("El acceso expiró", 401);
    }

    return payload;
  }

  function hashCodigo(codigo, nonce) {
    return crypto
      .createHmac("sha256", secret)
      .update(`${nonce}:${codigo}`)
      .digest("hex");
  }

  function compararHashes(hashA, hashB) {
    const bufferA = Buffer.from(String(hashA || ""));
    const bufferB = Buffer.from(String(hashB || ""));

    return (
      bufferA.length === bufferB.length &&
      crypto.timingSafeEqual(bufferA, bufferB)
    );
  }

  function generarCodigo() {
    return crypto.randomInt(0, 10000)
      .toString()
      .padStart(4, "0");
  }

  function ocultarCorreo(correo) {
    const [usuario, dominio] = String(correo).split("@");

    if (!usuario || !dominio) {
      return "correo registrado";
    }

    return `${usuario.slice(0, 2)}***@${dominio}`;
  }

  async function obtenerUsuarioActivo(idUsuario) {
    const [[usuario]] = await pool.execute(
      `
      SELECT
        id_usuario,
        id_plantel,
        nombre,
        apellidos,
        correo,
        status
      FROM usuarios
      WHERE id_usuario = ?
      LIMIT 1
      `,
      [idUsuario]
    );

    if (!usuario) {
      throw crearError("Usuario no encontrado", 404);
    }

    if (usuario.status !== "Activo") {
      throw crearError("El usuario está inactivo", 403);
    }

    if (!usuario.id_plantel) {
      throw crearError(
        "El usuario no tiene un plantel asignado",
        409
      );
    }

    if (!usuario.correo) {
      throw crearError(
        "El usuario no tiene correo registrado",
        409
      );
    }

    return usuario;
  }

  /**
   * AppSheet llama este endpoint usando x-api-token.
   * Genera un ticket temporal para abrir Lovable.
   */
  async function crearAccesoHandler(req, res, next) {
    try {
      const idUsuario = String(
        req.body?.id_usuario || ""
      ).trim();

      if (!idUsuario) {
        return res.status(400).json({
          ok: false,
          error: "id_usuario es requerido"
        });
      }

      const usuario = await obtenerUsuarioActivo(idUsuario);
      const now = Math.floor(Date.now() / 1000);

      const launchToken = firmar({
        purpose: "caja-launch",
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel,
        nonce: crypto.randomUUID(),
        iat: now,
        exp: now + LAUNCH_SECONDS
      });

      logger.info("Acceso temporal de Caja creado", {
        evento: "CAJA_AUTH_LAUNCH_CREATED",
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel
      });

      return res.json({
        ok: true,
        launch_token: launchToken,
        expira_en_segundos: LAUNCH_SECONDS
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * Lovable manda launch_token.
   * El backend envía el código por correo.
   */
  async function solicitarCodigoHandler(req, res, next) {
    try {
      const launchToken = String(
        req.body?.launch_token || ""
      ).trim();

      if (!launchToken) {
        return res.status(400).json({
          ok: false,
          error: "launch_token es requerido"
        });
      }

      const launchPayload = verificarFirma(launchToken);

      if (launchPayload.purpose !== "caja-launch") {
        throw crearError("Tipo de acceso inválido", 401);
      }

      const usuario = await obtenerUsuarioActivo(
        launchPayload.id_usuario
      );

      if (usuario.id_plantel !== launchPayload.id_plantel) {
        throw crearError(
          "El plantel del usuario cambió",
          409
        );
      }

      const ultimoEnvio =
        enviosRecientes.get(usuario.id_usuario) || 0;

      if (Date.now() - ultimoEnvio < 60_000) {
        throw crearError(
          "Espera un minuto antes de solicitar otro código",
          429
        );
      }

      const codigo = generarCodigo();
      const nonce = crypto.randomUUID();
      const now = Math.floor(Date.now() / 1000);

      const challengeToken = firmar({
        purpose: "caja-challenge",
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel,
        codigo_hash: hashCodigo(codigo, nonce),
        nonce,
        iat: now,
        exp: now + CHALLENGE_SECONDS
      });

      await sendAccessCode({
        correo: usuario.correo,
        nombre: `${usuario.nombre} ${usuario.apellidos}`.trim(),
        codigo
      });

      enviosRecientes.set(usuario.id_usuario, Date.now());
      intentosCodigo.set(nonce, 0);

      logger.info("Código de acceso de Caja enviado", {
        evento: "CAJA_AUTH_CODE_SENT",
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel
      });

      return res.json({
        ok: true,
        challenge_token: challengeToken,
        correo_mascara: ocultarCorreo(usuario.correo),
        expira_en_segundos: CHALLENGE_SECONDS
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * Lovable manda challenge_token + código.
   * Devuelve la sesión definitiva.
   */
  async function validarCodigoHandler(req, res, next) {
    try {
      const challengeToken = String(
        req.body?.challenge_token || ""
      ).trim();

      const codigo = String(
        req.body?.codigo || ""
      ).trim();

      if (!challengeToken || !/^\d{4}$/.test(codigo)) {
        return res.status(400).json({
          ok: false,
          error: "Código o challenge_token inválido"
        });
      }

      const challenge = verificarFirma(challengeToken);

      if (challenge.purpose !== "caja-challenge") {
        throw crearError("Tipo de acceso inválido", 401);
      }

      const intentos =
        intentosCodigo.get(challenge.nonce) || 0;

      if (intentos >= 5) {
        throw crearError(
          "Demasiados intentos. Solicita otro código.",
          429
        );
      }

      const hashIngresado = hashCodigo(
        codigo,
        challenge.nonce
      );

      if (
        !compararHashes(
          hashIngresado,
          challenge.codigo_hash
        )
      ) {
        intentosCodigo.set(
          challenge.nonce,
          intentos + 1
        );

        logger.warn("Código de Caja incorrecto", {
          evento: "CAJA_AUTH_CODE_FAILED",
          id_usuario: challenge.id_usuario,
          intento: intentos + 1
        });

        throw crearError("Código incorrecto", 401);
      }

      const usuario = await obtenerUsuarioActivo(
        challenge.id_usuario
      );

      if (usuario.id_plantel !== challenge.id_plantel) {
        throw crearError(
          "El plantel del usuario cambió",
          409
        );
      }

      intentosCodigo.delete(challenge.nonce);

      const now = Math.floor(Date.now() / 1000);

      const accessToken = firmar({
        purpose: "caja-session",
        rol: "Caja",
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel,
        session_id: crypto.randomUUID(),
        iat: now,
        exp: now + SESSION_SECONDS
      });

      logger.info("Sesión de Caja iniciada", {
        evento: "CAJA_AUTH_LOGIN_SUCCESS",
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel
      });

      return res.json({
        ok: true,
        access_token: accessToken,
        expira_en_segundos: SESSION_SECONDS,
        usuario: {
          id_usuario: usuario.id_usuario,
          nombre:
            `${usuario.nombre} ${usuario.apellidos}`.trim(),
          id_plantel: usuario.id_plantel,
          rol: "Caja"
        }
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * Middleware para endpoints usados por Lovable Caja.
   */
  async function requireCajaToken(req, res, next) {
    try {
      const authorization =
        req.headers.authorization || "";

      if (!authorization.startsWith("Bearer ")) {
        return res.status(401).json({
          ok: false,
          error: "Sesión de Caja requerida"
        });
      }

      const token = authorization.slice(7).trim();
      const payload = verificarFirma(token);

      if (
        payload.purpose !== "caja-session" ||
        payload.rol !== "Caja"
      ) {
        throw crearError(
          "Sesión no autorizada para Caja",
          403
        );
      }

      const usuario = await obtenerUsuarioActivo(
        payload.id_usuario
      );

      if (usuario.id_plantel !== payload.id_plantel) {
        throw crearError(
          "La sesión ya no corresponde al plantel actual",
          401
        );
      }

      req.caja = {
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel,
        nombre:
          `${usuario.nombre} ${usuario.apellidos}`.trim(),
        rol: "Caja",
        session_id: payload.session_id
      };

      next();
    } catch (error) {
      logger.warn("Acceso de Caja rechazado", {
        evento: "CAJA_AUTH_REQUEST_REJECTED",
        path: req.path,
        error: error.message
      });

      return res
        .status(error.statusCode || 401)
        .json({
          ok: false,
          error: error.message || "Sesión inválida"
        });
    }
  }

  async function sesionHandler(req, res) {
    return res.json({
      ok: true,
      usuario: req.caja
    });
  }

  return {
    crearAccesoHandler,
    solicitarCodigoHandler,
    validarCodigoHandler,
    requireCajaToken,
    sesionHandler
  };
};