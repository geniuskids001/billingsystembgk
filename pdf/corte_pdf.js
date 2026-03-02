const PDFDocument = require("pdfkit");
const path = require("path");

async function generateCortePDF(corte) {
  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({
        size: "LETTER",
        margin: 50,
        bufferPages: true
      });

      const chunks = [];
      doc.on("data", c => chunks.push(c));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      doc.on("error", reject);

      // ── Paleta ────────────────────────────────────────────────────
      const COLOR           = "#00739A";  // azul corporativo
      const GRAY            = "#666666";
      const LIGHT_GRAY      = "#F8F9FA";
      const BORDER_GRAY     = "#CCCCCC";

      const C_TARJETA       = "#00739A";  // azul
      const C_TRANSFERENCIA = "#E87722";  // naranja
      const C_EFECTIVO      = "#1A8C4E";  // verde
      const C_TOTAL         = "#222222";  // negro
      const BG_TOTAL        = "#E4E4E4";  // gris resaltado
      const C_GASTOS        = "#CC0000";  // rojo
      const BG_GASTOS       = "#FFF0F0";
      const C_NETO          = "#0066CC";  // azul vistoso
      const BG_NETO         = "#E0EDFF";

      const logoPath = path.join(__dirname, "../assets/businesslogo.png");

      /* ── ICONOS (primitivas PDFKit) ─────────────────────────────── */

      const drawCircleBg = (cx, cy, r, color) => {
        doc.save().circle(cx, cy, r).fillOpacity(0.13).fill(color).restore();
      };

      const iconTarjeta = (cx, cy, color) => {
        drawCircleBg(cx, cy, 13, color);
        doc.save().strokeColor(color).lineWidth(1.2);
        doc.roundedRect(cx - 9, cy - 6, 18, 12, 2).stroke();
        doc.save().fillOpacity(0.35).fillColor(color)
          .rect(cx - 9, cy - 2.5, 18, 3).fill().restore();
        doc.restore();
      };

      const iconEfectivo = (cx, cy, color) => {
        drawCircleBg(cx, cy, 13, color);
        doc.save().strokeColor(color).lineWidth(1.2);
        doc.roundedRect(cx - 10, cy - 6, 20, 12, 1.5).stroke();
        doc.fillColor(color).fontSize(8).font("Helvetica-Bold")
          .text("$", cx - 3, cy - 5.5, { lineBreak: false });
        doc.restore();
      };

      const iconTransferencia = (cx, cy, color) => {
        drawCircleBg(cx, cy, 13, color);
        doc.save().strokeColor(color).lineWidth(1.5);
        doc.moveTo(cx - 7, cy - 2.5).lineTo(cx + 5, cy - 2.5).stroke();
        doc.moveTo(cx + 2, cy - 5.5).lineTo(cx + 5, cy - 2.5).lineTo(cx + 2, cy + 0.5).stroke();
        doc.moveTo(cx + 7, cy + 2.5).lineTo(cx - 5, cy + 2.5).stroke();
        doc.moveTo(cx - 2, cy - 0.5).lineTo(cx - 5, cy + 2.5).lineTo(cx - 2, cy + 5.5).stroke();
        doc.restore();
      };

      const iconTotal = (cx, cy, color) => {
        drawCircleBg(cx, cy, 13, color);
        doc.save().fillColor(color).fontSize(15).font("Helvetica-Bold")
          .text("Σ", cx - 5.5, cy - 8.5, { lineBreak: false });
        doc.restore();
      };

      const iconGastos = (cx, cy, color) => {
        drawCircleBg(cx, cy, 13, color);
        doc.save().strokeColor(color).lineWidth(1.5);
        doc.moveTo(cx, cy - 7).lineTo(cx, cy + 4).stroke();
        doc.moveTo(cx - 4, cy + 1).lineTo(cx, cy + 5).lineTo(cx + 4, cy + 1).stroke();
        doc.moveTo(cx - 5, cy - 5).lineTo(cx + 5, cy - 5).stroke();
        doc.restore();
      };

      const iconNeto = (cx, cy, color) => {
        drawCircleBg(cx, cy, 13, color);
        doc.save().strokeColor(color).lineWidth(2);
        doc.moveTo(cx - 6, cy).lineTo(cx - 1, cy + 5).lineTo(cx + 6, cy - 5).stroke();
        doc.restore();
      };

      /* ── HEADER ──────────────────────────────────────────────────
         Derecha (de arriba a abajo):
           fecha · usuario · CORTE DE CAJA · folio · plantel
         → al doblar la hoja quedan visibles fecha y usuario.
      ─────────────────────────────────────────────────────────── */

      if (!corte.fecha) throw new Error("Corte sin fecha - datos inconsistentes");

      const fechaCorte = new Intl.DateTimeFormat("es-MX", {
        timeZone: "America/Mexico_City",
        weekday: "long",
        year: "numeric",
        month: "long",
        day: "numeric"
      }).format(new Date(corte.fecha));
      const fechaCorteFormateada = fechaCorte.charAt(0).toUpperCase() + fechaCorte.slice(1);
      const nombreResponsable = corte.usuario_nombre_completo || "Sin asignar";

      doc.image(logoPath, 50, 50, { width: 70 });

      // Fecha y usuario ENCIMA del título (para doblez)
      doc.fillColor(GRAY).fontSize(8).font("Helvetica")
        .text(fechaCorteFormateada, 200, 50, { align: "right" })
        .text(nombreResponsable,    200, 62, { align: "right" });

      // Título
      doc.fillColor(COLOR).fontSize(22).font("Helvetica-Bold")
        .text("CORTE DE CAJA", 200, 76, { align: "right" });

      // Folio + Plantel debajo del título sin encimarse
      doc.fillColor(GRAY).fontSize(9).font("Helvetica")
        .text(`Folio: ${corte.id_corte || "N/A"}`,        200, 106, { align: "right" })
        .text(`Plantel: ${corte.nombre_plantel || "N/A"}`, 200, 118, { align: "right" });

      doc.moveTo(50, 140).lineTo(562, 140).lineWidth(1.5).stroke(COLOR);

      /* ── CUANDO · QUIÉN · DÓNDE ──────────────────────────────── */

      const infoY = 155;

      doc.fillColor(GRAY).fontSize(8).font("Helvetica").text("CUANDO", 50, infoY);
      doc.fillColor("#333333").fontSize(9).font("Helvetica-Bold")
        .text(fechaCorteFormateada, 50, infoY + 12, { width: 165 });

      doc.fillColor(GRAY).fontSize(8).font("Helvetica").text("QUIÉN", 230, infoY);
      doc.fillColor("#333333").fontSize(9).font("Helvetica-Bold")
        .text(nombreResponsable, 230, infoY + 12, { width: 165 });

      doc.fillColor(GRAY).fontSize(8).font("Helvetica").text("DÓNDE", 410, infoY);
      doc.fillColor("#333333").fontSize(9).font("Helvetica-Bold")
        .text(corte.nombre_plantel || "N/A", 410, infoY + 12, { width: 145 });

      /* ── CUÁNTO — 6 tarjetas ─────────────────────────────────── */

      const sectionY = infoY + 52;
      doc.fillColor("#333333").fontSize(10).font("Helvetica-Bold")
        .text("CUÁNTO", 50, sectionY);

      const cardsTop = sectionY + 20;
      const cardW    = 157;
      const cardH    = 72;
      const cardGap  = 10;

      const drawCard = (x, y, label, amount, cardColor, bgColor, iconFn) => {
        doc.rect(x, y, cardW, cardH).lineWidth(2).fillAndStroke(bgColor, cardColor);
        if (iconFn) iconFn(x + 18, y + 22, cardColor);
        doc.fillColor(GRAY).fontSize(7.5).font("Helvetica")
          .text(label, x + 10, y + 10, { width: cardW - 20 });
        doc.fillColor(cardColor).fontSize(17).font("Helvetica-Bold")
          .text(`$${Number(amount || 0).toFixed(2)}`, x + 10, y + 42,
                { width: cardW - 20, align: "right" });
      };

      // Fila 1: Tarjeta | Transferencia | Efectivo
      drawCard(50,                     cardsTop, "TARJETA",       corte.total_tarjeta,       C_TARJETA,       LIGHT_GRAY, iconTarjeta);
      drawCard(50 + cardW + cardGap,   cardsTop, "TRANSFERENCIA", corte.total_transferencia, C_TRANSFERENCIA, "#FFF6EE",  iconTransferencia);
      drawCard(50 + (cardW+cardGap)*2, cardsTop, "EFECTIVO",      corte.total_efectivo,      C_EFECTIVO,      "#F0FAF4",  iconEfectivo);

      // Fila 2: Total | Gastos | Neto
      const row2Top = cardsTop + cardH + 12;
      drawCard(50,                     row2Top, "TOTAL INGRESOS",  corte.total,               C_TOTAL,  BG_TOTAL,  iconTotal);
      drawCard(50 + cardW + cardGap,   row2Top, "GASTOS EFECTIVO", corte.gastos_efectivo,     C_GASTOS, BG_GASTOS, iconGastos);
      drawCard(50 + (cardW+cardGap)*2, row2Top, "EFECTIVO NETO",   corte.total_efectivo_neto, C_NETO,   BG_NETO,   iconNeto);

      /* ── ANÁLISIS DE RECIBOS ─────────────────────────────────── */

      const tableLeft  = 50;
      const tableWidth = 512;
      const tableTop   = row2Top + cardH + 30;
      const colWidth   = 82;

      doc.fillColor("#333333").fontSize(10).font("Helvetica-Bold")
        .text("ANÁLISIS DE RECIBOS", 50, tableTop - 20);

      doc.rect(tableLeft, tableTop, tableWidth, 22).fill(LIGHT_GRAY);
      doc.fillColor(COLOR).fontSize(8).font("Helvetica-Bold")
        .text("ESTADO",    tableLeft + 10,  tableTop + 7, { width: 100 })
        .text("TARJETA",   tableLeft + 120, tableTop + 7, { width: colWidth, align: "right" })
        .text("TRANSFER.", tableLeft + 210, tableTop + 7, { width: colWidth, align: "right" })
        .text("EFECTIVO",  tableLeft + 300, tableTop + 7, { width: colWidth, align: "right" })
        .text("TOTAL",     tableLeft + 420, tableTop + 7, { width: colWidth, align: "right" });

      let matrixY = tableTop + 30;

      const drawMatrixRow = (label, rowData, index) => {
        if (index % 2 === 0)
          doc.rect(tableLeft, matrixY - 5, tableWidth, 20).fill("#FAFBFC");

        doc.fillColor("#333333").fontSize(9).font("Helvetica")
          .text(label,                           tableLeft + 10,  matrixY, { width: 100 })
          .text(`${rowData.Tarjeta || 0}`,       tableLeft + 120, matrixY, { width: colWidth, align: "right" })
          .text(`${rowData.Transferencia || 0}`, tableLeft + 210, matrixY, { width: colWidth, align: "right" })
          .text(`${rowData.Efectivo || 0}`,      tableLeft + 300, matrixY, { width: colWidth, align: "right" });

        doc.fillColor(COLOR).font("Helvetica-Bold")
          .text(`${rowData.total_fila || 0}`, tableLeft + 420, matrixY, { width: colWidth, align: "right" });

        doc.font("Helvetica").fillColor("#333333");
        matrixY += 20;
      };

      drawMatrixRow("Emitidos",   corte.recibos_matrix?.Emitido   || {}, 0);
      drawMatrixRow("Cancelados", corte.recibos_matrix?.Cancelado || {}, 1);

      matrixY += 5;
      doc.moveTo(tableLeft, matrixY).lineTo(tableLeft + tableWidth, matrixY)
        .lineWidth(0.5).stroke(BORDER_GRAY);
      matrixY += 10;

      doc.rect(tableLeft, matrixY - 5, tableWidth, 20).fill("#E8F4F8");
      doc.fillColor(COLOR).fontSize(9).font("Helvetica-Bold")
        .text("TOTALES",                                 tableLeft + 10,  matrixY, { width: 100 })
        .text(`${corte.totales_columnas?.Tarjeta || 0}`,       tableLeft + 120, matrixY, { width: colWidth, align: "right" })
        .text(`${corte.totales_columnas?.Transferencia || 0}`, tableLeft + 210, matrixY, { width: colWidth, align: "right" })
        .text(`${corte.totales_columnas?.Efectivo || 0}`,      tableLeft + 300, matrixY, { width: colWidth, align: "right" })
        .text(`${corte.total_global_recibos || 0}`,            tableLeft + 420, matrixY, { width: colWidth, align: "right" });

      /* ── FOOTER ──────────────────────────────────────────────── */

      doc.fillColor(GRAY).fontSize(8).font("Helvetica")
        .text("Este documento es un comprobante de corte de caja válido.", 50, 690,
              { align: "center", width: 512 });

      const footerY = 750;

      // ⚠️ Sin esto PDFKit crea segunda página
      doc.page.margins.bottom = 0;

      doc.rect(0, footerY, doc.page.width, 42).fill(COLOR);
      doc.moveTo(0, footerY).lineTo(doc.page.width, footerY)
        .lineWidth(1).stroke("#FFFFFF");

      if (corte.razon_social || corte.rfc || corte.ubicacion) {
        doc.fillColor("#FFFFFF").fontSize(8).font("Helvetica");

        if (corte.razon_social)
          doc.text(corte.razon_social, 50, footerY + 14, { width: 180, align: "left" });
        if (corte.rfc)
          doc.text(`RFC: ${corte.rfc}`, 230, footerY + 14, { width: 150, align: "center" });
        if (corte.ubicacion)
          doc.text(corte.ubicacion, 380, footerY + 14, { width: 182, align: "right" });
      }

      doc.end();

    } catch (err) {
      reject(err);
    }
  });
}

module.exports = { generateCortePDF };