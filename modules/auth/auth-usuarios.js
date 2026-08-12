const crypto = require("crypto");
const { promisify } = require("util");

const scryptAsync = promisify(crypto.scrypt);

module.exports = function authUsuariosFactory({
  pool,
  logger,
  secret,
  sendAuthCode
}) {

  const SESSION_SECONDS = 12 * 60 * 60;
  const CODE_SECONDS = 10 * 60;

  const MAX_PIN_INTENTOS = 5;
  const BLOQUEO_MINUTOS = 15;

  // ============================================================
  // HELPERS
  // ============================================================

  function crearError(message, statusCode = 400) {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
  }

  function base64Url(value) {
    return Buffer.from(value).toString("base64url");
  }

  function firmar(payload) {
    const headerEncoded = base64Url(
      JSON.stringify({
        alg: "HS256",
        typ: "BGK-AUTH"
      })
    );

    const payloadEncoded = base64Url(
      JSON.stringify(payload)
    );

    const unsignedToken =
      `${headerEncoded}.${payloadEncoded}`;

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

    const [
      headerEncoded,
      payloadEncoded,
      signature
    ] = partes;

    const unsignedToken =
      `${headerEncoded}.${payloadEncoded}`;

    const expected = crypto
      .createHmac("sha256", secret)
      .update(unsignedToken)
      .digest("base64url");

    const a = Buffer.from(signature);
    const b = Buffer.from(expected);

    if (
      a.length !== b.length ||
      !crypto.timingSafeEqual(a, b)
    ) {
      throw crearError("Token inválido", 401);
    }

    let payload;

    try {
      payload = JSON.parse(
        Buffer
          .from(payloadEncoded, "base64url")
          .toString("utf8")
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

  // ============================================================
  // PIN HASH
  // ============================================================

  function validarFormatoPin(pin) {
    return /^\d{4}$|^\d{6}$/.test(pin);
  }

  async function hashPin(pin) {
    const salt = crypto
      .randomBytes(16)
      .toString("hex");

    const derived = await scryptAsync(
      pin,
      salt,
      64
    );

    return `scrypt$${salt}$${derived.toString("hex")}`;
  }

  async function verificarPin(pin, storedHash) {
    const partes =
      String(storedHash || "").split("$");

    if (
      partes.length !== 3 ||
      partes[0] !== "scrypt"
    ) {
      return false;
    }

    const [, salt, hash] = partes;

    const derived = await scryptAsync(
      pin,
      salt,
      64
    );

    const actual = derived;
    const esperado = Buffer.from(hash, "hex");

    return (
      actual.length === esperado.length &&
      crypto.timingSafeEqual(actual, esperado)
    );
  }

  // ============================================================
  // CÓDIGO EMAIL
  // ============================================================

  function generarCodigo() {
    return crypto
      .randomInt(0, 1_000_000)
      .toString()
      .padStart(6, "0");
  }

  function hashCodigo(codigo, nonce) {
    return crypto
      .createHmac("sha256", secret)
      .update(`${nonce}:${codigo}`)
      .digest("hex");
  }

  function compararHash(a, b) {
    const ba = Buffer.from(String(a || ""));
    const bb = Buffer.from(String(b || ""));

    return (
      ba.length === bb.length &&
      crypto.timingSafeEqual(ba, bb)
    );
  }

  // ============================================================
  // USUARIO
  // ============================================================

  async function obtenerUsuario({
    idUsuario,
    correo
  }) {

    const campo =
      idUsuario ? "u.id_usuario" : "u.correo";

    const valor =
      idUsuario || correo;

    if (!valor) {
      throw crearError(
        "id_usuario o correo es requerido",
        400
      );
    }

    const [[usuario]] = await pool.execute(
      `
      SELECT
        u.id_usuario,
        u.id_plantel,
        u.nombre,
        u.apellidos,
        u.correo,
        u.status,
        r.nombre AS rol,

        ua.pin_hash,
        ua.intentos_fallidos,
        ua.pin_longitud,
        ua.bloqueado_hasta

      FROM usuarios u

      LEFT JOIN roles r
        ON r.id_rol = u.id_rol

      LEFT JOIN usuarios_auth ua
        ON ua.id_usuario = u.id_usuario

      WHERE ${campo} = ?
      LIMIT 1
      `,
      [valor]
    );

    if (!usuario) {
      throw crearError(
        "Usuario no encontrado",
        404
      );
    }

    if (usuario.status !== "Activo") {
      throw crearError(
        "El usuario está inactivo",
        403
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

  // ============================================================
  // SOLICITAR CÓDIGO PARA ESTABLECER / RESTABLECER PIN
  // ============================================================

  async function solicitarCodigoPin(
    req,
    res,
    next
  ) {
    try {

      const idUsuario = String(
        req.body?.id_usuario || ""
      ).trim();

      const correo = String(
        req.body?.correo || ""
      )
        .trim()
        .toLowerCase();

      const accion = String(
        req.body?.accion || ""
      );

      if (
        !["Establecer", "Restablecer"]
          .includes(accion)
      ) {
        throw crearError(
          "Acción inválida",
          400
        );
      }

      const usuario = await obtenerUsuario({
        idUsuario,
        correo
      });

      if (
        accion === "Establecer" &&
        usuario.pin_hash
      ) {
        throw crearError(
          "El usuario ya tiene PIN. Utiliza restablecer PIN.",
          409
        );
      }

      if (
        accion === "Restablecer" &&
        !usuario.pin_hash
      ) {
        throw crearError(
          "El usuario todavía no tiene un PIN establecido",
          409
        );
      }

      const codigo = generarCodigo();
      const nonce = crypto.randomUUID();

      const expira =
        new Date(Date.now() + CODE_SECONDS * 1000);

      await pool.execute(
        `
        INSERT INTO usuarios_auth (
          id_usuario,
          codigo_hash,
          codigo_nonce,
          codigo_expira,
          codigo_intentos
        )
        VALUES (?, ?, ?, ?, 0)

        ON DUPLICATE KEY UPDATE
          codigo_hash = VALUES(codigo_hash),
          codigo_nonce = VALUES(codigo_nonce),
          codigo_expira = VALUES(codigo_expira),
          codigo_intentos = 0
        `,
        [
          usuario.id_usuario,
          hashCodigo(codigo, nonce),
          nonce,
          expira
        ]
      );

      const now =
        Math.floor(Date.now() / 1000);

      const challengeToken = firmar({
        purpose: "pin-change",
        id_usuario: usuario.id_usuario,
        accion,
        nonce,
        iat: now,
        exp: now + CODE_SECONDS
      });

      await sendAuthCode({
        correo: usuario.correo,
        nombre:
          `${usuario.nombre} ${usuario.apellidos}`.trim(),
        codigo,
        accion
      });

      logger.info(
        "Código para PIN enviado",
        {
          evento: "AUTH_PIN_CODE_SENT",
          id_usuario: usuario.id_usuario,
          accion
        }
      );

      return res.json({
        ok: true,
        challenge_token: challengeToken,
        expira_en_segundos: CODE_SECONDS
      });

    } catch (error) {
      next(error);
    }
  }

  // ============================================================
  // CONFIRMAR CÓDIGO + GUARDAR NUEVO PIN
  // ============================================================

  async function guardarPinHandler(
    req,
    res,
    next
  ) {
    try {

      const challengeToken = String(
        req.body?.challenge_token || ""
      ).trim();

      const codigo = String(
        req.body?.codigo || ""
      ).trim();

      const pin = String(
        req.body?.pin || ""
      ).trim();

      if (!challengeToken) {
        throw crearError(
          "challenge_token es requerido",
          400
        );
      }

      if (!/^\d{6}$/.test(codigo)) {
        throw crearError(
          "Código inválido",
          400
        );
      }

      if (!validarFormatoPin(pin)) {
        throw crearError(
          "El PIN debe tener 4 o 6 dígitos",
          400
        );
      }

      const challenge =
        verificarFirma(challengeToken);

      if (
        challenge.purpose !== "pin-change"
      ) {
        throw crearError(
          "Solicitud inválida",
          401
        );
      }

      const [[auth]] = await pool.execute(
  `
  SELECT
    codigo_hash,
    codigo_nonce,
    codigo_expira,
    codigo_intentos,
    (codigo_expira <= NOW()) AS expirado
  FROM usuarios_auth
  WHERE id_usuario = ?
  LIMIT 1
  `,
  [challenge.id_usuario]
);

      if (!auth) {
        throw crearError(
          "Solicitud de PIN no encontrada",
          404
        );
      }

      if (
        auth.codigo_nonce !== challenge.nonce
      ) {
        throw crearError(
          "El código ya no es válido",
          401
        );
      }

      if (!auth.codigo_expira || auth.expirado) {
  throw crearError(
    "El código expiró",
    401
  );
}

      if (Number(auth.codigo_intentos) >= 5) {
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
        !compararHash(
          hashIngresado,
          auth.codigo_hash
        )
      ) {

        await pool.execute(
          `
          UPDATE usuarios_auth
          SET codigo_intentos =
              codigo_intentos + 1
          WHERE id_usuario = ?
          `,
          [challenge.id_usuario]
        );

        throw crearError(
          "Código incorrecto",
          401
        );
      }

      const nuevoHash =
        await hashPin(pin);
        const pinLongitud = pin.length;

      await pool.execute(
  `
  UPDATE usuarios_auth
  SET
    pin_hash = ?,
    pin_longitud = ?,
    fecha_pin = NOW(),

    intentos_fallidos = 0,
    bloqueado_hasta = NULL,

    codigo_hash = NULL,
    codigo_nonce = NULL,
    codigo_expira = NULL,
    codigo_intentos = 0

  WHERE id_usuario = ?
  `,
  [
    nuevoHash,
    pinLongitud,
    challenge.id_usuario
  ]
);

      logger.info(
        "PIN establecido",
        {
          evento: "AUTH_PIN_CHANGED",
          id_usuario:
            challenge.id_usuario,
          accion:
            challenge.accion
        }
      );

      return res.json({
        ok: true,
        mensaje:
          challenge.accion === "Restablecer"
            ? "PIN restablecido correctamente"
            : "PIN establecido correctamente"
      });

    } catch (error) {
      next(error);
    }
  }

  // ============================================================
  // LOGIN CON PIN
  // ============================================================

  async function loginPinHandler(
    req,
    res,
    next
  ) {
    try {

      const idUsuario = String(
        req.body?.id_usuario || ""
      ).trim();

      const correo = String(
        req.body?.correo || ""
      )
        .trim()
        .toLowerCase();

      const pin = String(
        req.body?.pin || ""
      ).trim();

      if (!validarFormatoPin(pin)) {
        throw crearError(
          "PIN inválido",
          400
        );
      }

      const usuario = await obtenerUsuario({
        idUsuario,
        correo
      });

      if (!usuario.pin_hash) {
        throw crearError(
          "El usuario todavía no tiene un PIN establecido",
          409
        );
      }

      if (
        usuario.bloqueado_hasta &&
        new Date(usuario.bloqueado_hasta) >
          new Date()
      ) {
        throw crearError(
          "Acceso temporalmente bloqueado por intentos fallidos",
          429
        );
      }

      const valido =
        await verificarPin(
          pin,
          usuario.pin_hash
        );

      if (!valido) {

        const intentos =
          Number(usuario.intentos_fallidos || 0) + 1;

        if (intentos >= MAX_PIN_INTENTOS) {

          await pool.execute(
            `
            UPDATE usuarios_auth
            SET
              intentos_fallidos = 0,
              bloqueado_hasta =
                DATE_ADD(
                  NOW(),
                  INTERVAL ? MINUTE
                )
            WHERE id_usuario = ?
            `,
            [
              BLOQUEO_MINUTOS,
              usuario.id_usuario
            ]
          );

          throw crearError(
            `Demasiados intentos. Intenta nuevamente en ${BLOQUEO_MINUTOS} minutos.`,
            429
          );
        }

        await pool.execute(
          `
          UPDATE usuarios_auth
          SET intentos_fallidos = ?
          WHERE id_usuario = ?
          `,
          [
            intentos,
            usuario.id_usuario
          ]
        );

        throw crearError(
          "PIN incorrecto",
          401
        );
      }

      await pool.execute(
        `
        UPDATE usuarios_auth
        SET
          intentos_fallidos = 0,
          bloqueado_hasta = NULL
        WHERE id_usuario = ?
        `,
        [usuario.id_usuario]
      );

      const now =
        Math.floor(Date.now() / 1000);

      const accessToken = firmar({
        purpose: "user-session",
        id_usuario: usuario.id_usuario,
        session_id: crypto.randomUUID(),
        iat: now,
        exp: now + SESSION_SECONDS
      });

      logger.info(
        "Sesión iniciada con PIN",
        {
          evento: "AUTH_LOGIN_SUCCESS",
          id_usuario: usuario.id_usuario,
          rol: usuario.rol
        }
      );

      return res.json({
        ok: true,

        access_token: accessToken,

        expira_en_segundos:
          SESSION_SECONDS,

        usuario: {
          id_usuario: usuario.id_usuario,
          nombre:
            `${usuario.nombre} ${usuario.apellidos}`.trim(),
          id_plantel: usuario.id_plantel,
          rol: usuario.rol
        }
      });

    } catch (error) {
      next(error);
    }
  }

  // ============================================================
  // MIDDLEWARE GENERAL
  // ============================================================

  async function requireAuthToken(
    req,
    res,
    next
  ) {
    try {

      const authorization =
        req.headers.authorization || "";

      if (
        !authorization.startsWith("Bearer ")
      ) {
        throw crearError(
          "Sesión requerida",
          401
        );
      }

      const token =
        authorization.slice(7).trim();

      const payload =
        verificarFirma(token);

      if (
        payload.purpose !== "user-session"
      ) {
        throw crearError(
          "Sesión inválida",
          401
        );
      }

      const usuario =
        await obtenerUsuario({
          idUsuario: payload.id_usuario
        });

      req.usuario = {
        id_usuario: usuario.id_usuario,
        id_plantel: usuario.id_plantel,
        nombre:
          `${usuario.nombre} ${usuario.apellidos}`.trim(),
        rol: usuario.rol,
        session_id: payload.session_id
      };

      next();

    } catch (error) {

      logger.warn(
        "Sesión rechazada",
        {
          evento: "AUTH_REQUEST_REJECTED",
          path: req.path,
          error: error.message
        }
      );

      return res
        .status(error.statusCode || 401)
        .json({
          ok: false,
          error:
            error.message ||
            "Sesión inválida"
        });
    }
  }

  async function sesionHandler(req, res) {
    return res.json({
      ok: true,
      usuario: req.usuario
    });
  }


async function identificarUsuarioHandler(
  req,
  res,
  next
) {
  try {

    const idUsuario = String(
      req.body?.id_usuario || ""
    ).trim();

    const correo = String(
      req.body?.correo || ""
    )
      .trim()
      .toLowerCase();

    const usuario = await obtenerUsuario({
      idUsuario,
      correo
    });

    return res.json({
      ok: true,

      usuario: {
        id_usuario: usuario.id_usuario,
        nombre:
          `${usuario.nombre} ${usuario.apellidos}`.trim(),
        correo: usuario.correo,
        tiene_pin: !!usuario.pin_hash,
        pin_longitud:
          usuario.pin_longitud || null
      }
    });

  } catch (error) {
    next(error);
  }
}



  return {
  identificarUsuarioHandler,
  solicitarCodigoPin,
  guardarPinHandler,
  loginPinHandler,
  requireAuthToken,
  sesionHandler
};
};