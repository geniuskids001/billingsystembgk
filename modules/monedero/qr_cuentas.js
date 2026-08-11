const QRCode = require("qrcode");
const fs = require("fs");
const path = require("path");

module.exports = function qrCuentasFactory({
  pool,
  logger
}) {

  function crearError(message, statusCode = 400) {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function obtenerLogoDataUrl() {
    const logoPath = path.resolve(
      __dirname,
      "../../assets/genius_bites_logo.png"
    );

    if (!fs.existsSync(logoPath)) {
      return null;
    }

    return (
      "data:image/png;base64," +
      fs.readFileSync(logoPath).toString("base64")
    );
  }

  async function imprimirHandler(req, res, next) {
    try {

      // =========================================================
      // 1. USUARIO / SELECCIÓN DE APPSHEET
      // =========================================================
      const userEmail = String(
        req.query.user_email || ""
      )
        .trim()
        .toLowerCase();

      if (!userEmail) {
        throw crearError(
          "user_email es requerido",
          400
        );
      }

      const [[filtro]] = await pool.execute(
        `
        SELECT filtro_alumnos
        FROM filtros_usuario
        WHERE user_email = ?
        LIMIT 1
        `,
        [userEmail]
      );

      if (!filtro) {
        throw crearError(
          "No se encontró el filtro del usuario",
          404
        );
      }

      const idsAlumnos = [
        ...new Set(
          String(filtro.filtro_alumnos || "")
            .split(",")
            .map(id => id.trim())
            .filter(Boolean)
        )
      ];

      if (idsAlumnos.length === 0) {
        throw crearError(
          "No hay alumnos seleccionados para imprimir",
          400
        );
      }

      if (idsAlumnos.length > 300) {
        throw crearError(
          "Solo se pueden imprimir hasta 300 alumnos por lote",
          400
        );
      }

      // =========================================================
      // 2. ALUMNOS ACTIVOS + RELACIÓN ACTIVA + CUENTA ACTIVA
      // =========================================================
      const placeholders = idsAlumnos
        .map(() => "?")
        .join(",");

      const [alumnos] = await pool.execute(
        `
        SELECT
          a.id_alumno,

          CONCAT_WS(
            ' ',
            a.nombre,
            a.apellido_paterno,
            a.apellido_materno
          ) AS nombre_completo,

          g.nombre_grupo,
          g.etiqueta_grado,

          mca.qr_token

        FROM alumnos a

        JOIN monedero_cuenta_alumnos mca
          ON mca.id_alumno = a.id_alumno
         AND mca.status = 'Activo'

        JOIN monedero_cuentas mc
          ON mc.id_cuenta = mca.id_cuenta
         AND mc.status = 'Activo'

        LEFT JOIN grupos g
          ON g.id_grupo = a.id_grupo

        WHERE a.id_alumno IN (${placeholders})
          AND a.status = 'Activo'

        ORDER BY
          g.grado,
          g.nombre_grupo,
          a.apellido_paterno,
          a.apellido_materno,
          a.nombre
        `,
        idsAlumnos
      );

      if (alumnos.length === 0) {
        throw crearError(
          "Ninguno de los alumnos seleccionados tiene una cuenta activa",
          404
        );
      }

      // =========================================================
      // 3. GENERAR QR
      // =========================================================
      const logo = obtenerLogoDataUrl();
      const tarjetas = [];

      for (const alumno of alumnos) {

        if (!alumno.qr_token) {
          continue;
        }

        const qrDataUrl = await QRCode.toDataURL(
          alumno.qr_token,
          {
            errorCorrectionLevel: "H",
            margin: 1,
            width: 500
          }
        );

        tarjetas.push({
          nombre: alumno.nombre_completo,
          grupo:
            alumno.nombre_grupo ||
            alumno.etiqueta_grado ||
            "",
          qr: qrDataUrl
        });
      }

      if (tarjetas.length === 0) {
        throw crearError(
          "No hay códigos QR disponibles para imprimir",
          404
        );
      }

      // =========================================================
      // 4. CREAR HOJAS DE 9 TARJETAS
      // =========================================================
      const hojas = [];

      for (let i = 0; i < tarjetas.length; i += 9) {
        hojas.push(tarjetas.slice(i, i + 9));
      }

      const hojasHtml = hojas
        .map(hoja => {

          const cards = hoja
            .map(alumno => `
              <article class="card">

                <div class="qr-box">

                  <img
                    class="qr"
                    src="${alumno.qr}"
                    alt="Código QR"
                  >

                  ${
                    logo
                      ? `
                        <div class="logo">
                          <img src="${logo}" alt="">
                        </div>
                      `
                      : ""
                  }

                </div>

                <div class="nombre">
                  ${escapeHtml(alumno.nombre)}
                </div>

                ${
                  alumno.grupo
                    ? `
                      <div class="grupo">
                        ${escapeHtml(alumno.grupo)}
                      </div>
                    `
                    : ""
                }

              </article>
            `)
            .join("");

          return `
            <section class="hoja">
              ${cards}
            </section>
          `;
        })
        .join("");

      // =========================================================
      // 5. HTML
      // =========================================================
      const html = `
<!DOCTYPE html>
<html lang="es">

<head>

<meta charset="UTF-8">

<meta
  name="viewport"
  content="width=device-width, initial-scale=1"
>

<title>Tarjetas QR · Genius Bites</title>

<style>

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: #f4f6f5;
  font-family: Arial, Helvetica, sans-serif;
  color: #17352a;
}

/* TOOLBAR */

.toolbar {
  position: sticky;
  top: 0;
  z-index: 10;

  display: flex;
  align-items: center;
  justify-content: space-between;

  padding: 14px 20px;

  background: white;
  border-bottom: 1px solid #e3e8e5;

  box-shadow: 0 2px 8px rgba(0,0,0,.05);
}

.titulo {
  font-size: 18px;
  font-weight: 700;
}

.info {
  margin-top: 3px;
  color: #6b766f;
  font-size: 13px;
}

.boton {
  border: 0;
  border-radius: 9px;

  padding: 11px 18px;

  background: #1F8A5B;
  color: white;

  font-size: 15px;
  font-weight: 700;

  cursor: pointer;
}

/* HOJA CARTA */

.hoja {
  width: 216mm;
  height: 279mm;

  margin: 18px auto;
  padding: 8mm;

  background: white;

  display: grid;
  grid-template-columns: repeat(3, 1fr);
  grid-template-rows: repeat(3, 1fr);

  gap: 4mm;

  break-after: page;
}

.hoja:last-child {
  break-after: auto;
}

/* TARJETA */

.card {
  border: 1.2px solid #cfd8d3;
  border-radius: 4mm;

  padding: 5mm 4mm;

  display: flex;
  flex-direction: column;
  align-items: center;

  text-align: center;

  overflow: hidden;
}

.qr-box {
  position: relative;

  width: 48mm;
  height: 48mm;
}

.qr {
  width: 100%;
  height: 100%;
  display: block;
}

/* LOGO */

.logo {
  position: absolute;

  left: 50%;
  top: 50%;

  transform: translate(-50%, -50%);

  width: 11mm;
  height: 11mm;

  padding: 1.2mm;

  background: white;
  border-radius: 2mm;

  display: flex;
  align-items: center;
  justify-content: center;
}

.logo img {
  max-width: 100%;
  max-height: 100%;
}

/* DATOS */

.nombre {
  margin-top: 3mm;

  font-size: 12pt;
  font-weight: 700;
  line-height: 1.15;
}

.grupo {
  margin-top: 1.3mm;

  color: #68736d;

  font-size: 9.5pt;
  font-weight: 600;
}

/* IMPRESIÓN */

@page {
  size: Letter;
  margin: 0;
}

@media print {

  body {
    background: white;
  }

  .toolbar {
    display: none;
  }

  .hoja {
    margin: 0;
  }

}

</style>

</head>

<body>

<header class="toolbar">

  <div>

    <div class="titulo">
      Genius Bites · Tarjetas QR
    </div>

    <div class="info">
      ${tarjetas.length}
      ${
        tarjetas.length === 1
          ? "tarjeta lista"
          : "tarjetas listas"
      }
      · ${hojas.length}
      ${
        hojas.length === 1
          ? "hoja"
          : "hojas"
      }
    </div>

  </div>

  <button
    class="boton"
    onclick="window.print()"
  >
    🖨️ Imprimir
  </button>

</header>

${hojasHtml}

</body>

</html>
      `;

      logger.info(
        "Página QR generada",
        {
          evento: "MONEDERO_QR_PRINT",
          user_email: userEmail,
          solicitados: idsAlumnos.length,
          impresos: tarjetas.length,
          hojas: hojas.length
        }
      );

      res.set(
        "Content-Type",
        "text/html; charset=utf-8"
      );

      res.set(
        "Cache-Control",
        "no-store"
      );

      return res.send(html);

    } catch (error) {
      next(error);
    }
  }

  return {
    imprimirHandler
  };
};