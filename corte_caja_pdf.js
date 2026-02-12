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

      const COLOR = "#00739A";
      const GRAY = "#666666";
      const LIGHT_GRAY = "#F8F9FA";
      const BORDER_GRAY = "#CCCCCC";
      const HIGHLIGHT_BG = "#E8F4F8";
      const logoPath = path.join(__dirname, "assets/businesslogo.png");

      /* ================= HEADER - CONSISTENTE CON RECIBO ================= */
      doc.image(logoPath, 50, 50, { width: 70 });

      doc
        .fillColor(COLOR)
        .fontSize(22)
        .font("Helvetica-Bold")
        .text("CORTE DE CAJA", 200, 50, { align: "right" });

      doc
        .fillColor(GRAY)
        .fontSize(9)
        .font("Helvetica")
        .text(`Folio: ${corte.id_corte || 'N/A'}`, 200, 80, { align: "right" })
        .text(`Plantel: ${corte.nombre_plantel || 'N/A'}`, 200, 93, {
          align: "right"
        });

      doc
        .moveTo(50, 135)
        .lineTo(562, 135)
        .lineWidth(1.5)
        .stroke(COLOR);

      /* ================= JERARQUÍA VISUAL: CUÁNDO ================= */
      doc
        .fillColor("#333333")
        .fontSize(9)
        .font("Helvetica")
        .text("FECHA DEL CORTE", 50, 155, {
          width: 512,
          align: "right"
        });

      // Validación de fecha
      if (!corte.fecha) {
        throw new Error("Corte sin fecha - datos inconsistentes");
      }

      const fechaCorte = new Intl.DateTimeFormat("es-MX", {
        timeZone: "America/Mexico_City",
        weekday: "long",
        year: "numeric",
        month: "long",
        day: "numeric"
      }).format(new Date(corte.fecha));

      const fechaCorteFormateada =
        fechaCorte.charAt(0).toUpperCase() + fechaCorte.slice(1);

      doc
        .fontSize(11)
        .font("Helvetica-Bold")
        .text(fechaCorteFormateada, 50, 170, {
          width: 512,
          align: "right"
        });

      /* ================= JERARQUÍA VISUAL: QUIÉN ================= */
      doc
        .fillColor("#333333")
        .fontSize(9)
        .font("Helvetica")
        .text("RESPONSABLE DEL CORTE", 50, 205);

      const nombreResponsable = corte.usuario_nombre_completo || 'Sin asignar';
      
      doc
        .fontSize(11)
        .font("Helvetica-Bold")
        .text(nombreResponsable, 50, 220, { width: 512 });

      /* ================= JERARQUÍA VISUAL: CUÁNTO - DESGLOSE COMPLETO ================= */
      
      doc
        .fillColor("#333333")
        .fontSize(10)
        .font("Helvetica-Bold")
        .text("RESUMEN FINANCIERO", 50, 250);

      const cardsTop = 275;
      const cardWidth = 157;
      const cardHeight = 70;
      const cardGap = 10;

      // ============ FILA 1: INGRESOS POR MÉTODO ============
      
      // Card 1: Tarjeta
      const card1X = 50;
      doc
        .rect(card1X, cardsTop, cardWidth, cardHeight)
        .lineWidth(2)
        .fillAndStroke(LIGHT_GRAY, COLOR);

      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text("TARJETA", card1X + 10, cardsTop + 10, { width: cardWidth - 20 });

      const totalTarjeta = Number(corte.total_tarjeta) || 0;
      doc
        .fillColor(COLOR)
        .fontSize(18)
        .font("Helvetica-Bold")
        .text(
          `$${totalTarjeta.toFixed(2)}`,
          card1X + 10,
          cardsTop + 28,
          { width: cardWidth - 20, align: "right" }
        );

      // Card 2: Transferencia
      const card2X = card1X + cardWidth + cardGap;
      doc
        .rect(card2X, cardsTop, cardWidth, cardHeight)
        .lineWidth(2)
        .fillAndStroke(LIGHT_GRAY, COLOR);

      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text("TRANSFERENCIA", card2X + 10, cardsTop + 10, { width: cardWidth - 20 });

      const totalTransferencia = Number(corte.total_transferencia) || 0;
      doc
        .fillColor(COLOR)
        .fontSize(18)
        .font("Helvetica-Bold")
        .text(
          `$${totalTransferencia.toFixed(2)}`,
          card2X + 10,
          cardsTop + 28,
          { width: cardWidth - 20, align: "right" }
        );

      // Card 3: Efectivo
      const card3X = card2X + cardWidth + cardGap;
      doc
        .rect(card3X, cardsTop, cardWidth, cardHeight)
        .lineWidth(2)
        .fillAndStroke(LIGHT_GRAY, COLOR);

      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text("EFECTIVO", card3X + 10, cardsTop + 10, { width: cardWidth - 20 });

      const totalEfectivo = Number(corte.total_efectivo) || 0;
      doc
        .fillColor(COLOR)
        .fontSize(18)
        .font("Helvetica-Bold")
        .text(
          `$${totalEfectivo.toFixed(2)}`,
          card3X + 10,
          cardsTop + 28,
          { width: cardWidth - 20, align: "right" }
        );

      // ============ FILA 2: TOTAL, GASTOS Y NETO (en orden lógico) ============
      const row2Top = cardsTop + cardHeight + 15;

      // Card 4: Total Ingresos
      doc
        .rect(card1X, row2Top, cardWidth, cardHeight)
        .lineWidth(2)
        .fillAndStroke(HIGHLIGHT_BG, COLOR);

      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text("TOTAL INGRESOS", card1X + 10, row2Top + 10, { width: cardWidth - 20 });

      const totalIngresos = Number(corte.total) || 0;
      doc
        .fillColor(COLOR)
        .fontSize(18)
        .font("Helvetica-Bold")
        .text(
          `$${totalIngresos.toFixed(2)}`,
          card1X + 10,
          row2Top + 28,
          { width: cardWidth - 20, align: "right" }
        );

      // Card 5: Gastos en Efectivo
      const gastosCardX = card1X + cardWidth + cardGap;
      const totalGastos = Number(corte.gastos_efectivo) || 0;
      
      doc
        .rect(gastosCardX, row2Top, cardWidth, cardHeight)
        .lineWidth(2)
        .fillAndStroke("#FFF5F5", "#CC6600");

      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text("GASTOS EFECTIVO", gastosCardX + 10, row2Top + 10, { width: cardWidth - 20 });

      doc
        .fillColor("#CC6600")
        .fontSize(18)
        .font("Helvetica-Bold")
        .text(
          `$${totalGastos.toFixed(2)}`,
          gastosCardX + 10,
          row2Top + 28,
          { width: cardWidth - 20, align: "right" }
        );

      // Card 6: Efectivo Neto (resultado final)
      const netoCardX = gastosCardX + cardWidth + cardGap;
      const efectivoNeto = Number(corte.total_efectivo_neto) || 0;
      
      doc
        .rect(netoCardX, row2Top, cardWidth, cardHeight)
        .lineWidth(2)
        .fillAndStroke(LIGHT_GRAY, "#009933");

      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text("EFECTIVO NETO", netoCardX + 10, row2Top + 10, { width: cardWidth - 20 });

      doc
        .fillColor("#009933")
        .fontSize(18)
        .font("Helvetica-Bold")
        .text(
          `$${efectivoNeto.toFixed(2)}`,
          netoCardX + 10,
          row2Top + 28,
          { width: cardWidth - 20, align: "right" }
        );

      // Ajustar siguiente sección
      const tableLeft = 50;
      const tableWidth = 512;
      let nextSectionTop = row2Top + cardHeight + 30;

      /* ================= ESTADO DE RECIBOS ================= */

      doc
        .fillColor("#333333")
        .fontSize(10)
        .font("Helvetica-Bold")
        .text("ANÁLISIS DE RECIBOS", 50, nextSectionTop);

      // Matriz de recibos
      const matrixTop = nextSectionTop + 25;
      const colWidth = 82;

      // Header de matriz
      doc.rect(tableLeft, matrixTop, tableWidth, 22).fill(LIGHT_GRAY);

      doc
        .fillColor(COLOR)
        .fontSize(8)
        .font("Helvetica-Bold")
        .text("ESTADO", tableLeft + 10, matrixTop + 7, { width: 100 })
        .text("TARJETA", tableLeft + 120, matrixTop + 7, { width: colWidth, align: "right" })
        .text("TRANSFER.", tableLeft + 210, matrixTop + 7, { width: colWidth, align: "right" })
        .text("EFECTIVO", tableLeft + 300, matrixTop + 7, { width: colWidth, align: "right" })
        .text("TOTAL", tableLeft + 420, matrixTop + 7, { width: colWidth, align: "right" });

      let matrixY = matrixTop + 30;

      const drawMatrixRow = (label, rowData, index) => {
        if (index % 2 === 0) {
          doc.rect(tableLeft, matrixY - 5, tableWidth, 20).fill("#FAFBFC");
        }

        doc
          .fillColor("#333333")
          .fontSize(9)
          .font("Helvetica")
          .text(label, tableLeft + 10, matrixY, { width: 100 });

        doc
          .font("Helvetica")
          .text(`${rowData.Tarjeta || 0}`, tableLeft + 120, matrixY, { width: colWidth, align: "right" })
          .text(`${rowData.Transferencia || 0}`, tableLeft + 210, matrixY, { width: colWidth, align: "right" })
          .text(`${rowData.Efectivo || 0}`, tableLeft + 300, matrixY, { width: colWidth, align: "right" });

        doc
          .fillColor(COLOR)
          .font("Helvetica-Bold")
          .text(`${rowData.total_fila || 0}`, tableLeft + 420, matrixY, { width: colWidth, align: "right" });

        doc.font("Helvetica").fillColor("#333333");
        matrixY += 20;
      };

      drawMatrixRow("Emitidos", corte.recibos_matrix?.Emitido || {}, 0);
      drawMatrixRow("Cancelados", corte.recibos_matrix?.Cancelado || {}, 1);

      // Línea antes de totales
      matrixY += 5;
      doc
        .moveTo(tableLeft, matrixY)
        .lineTo(tableLeft + tableWidth, matrixY)
        .lineWidth(0.5)
        .stroke(BORDER_GRAY);

      matrixY += 10;

      // Fila de totales
      doc.rect(tableLeft, matrixY - 5, tableWidth, 20).fill(HIGHLIGHT_BG);

      doc
        .fillColor(COLOR)
        .fontSize(9)
        .font("Helvetica-Bold")
        .text("TOTALES", tableLeft + 10, matrixY, { width: 100 })
        .text(`${corte.totales_columnas?.Tarjeta || 0}`, tableLeft + 120, matrixY, { width: colWidth, align: "right" })
        .text(`${corte.totales_columnas?.Transferencia || 0}`, tableLeft + 210, matrixY, { width: colWidth, align: "right" })
        .text(`${corte.totales_columnas?.Efectivo || 0}`, tableLeft + 300, matrixY, { width: colWidth, align: "right" })
        .text(`${corte.total_global_recibos || 0}`, tableLeft + 420, matrixY, { width: colWidth, align: "right" });

      /* ================= FOOTER PROFESIONAL - IGUAL AL RECIBO ================= */
      const footerY = 750;

      // Barra azul de fondo
      doc
        .rect(0, footerY, doc.page.width, 42)
        .fill(COLOR);

      // Línea decorativa superior
      doc
        .moveTo(0, footerY)
        .lineTo(doc.page.width, footerY)
        .lineWidth(1)
        .stroke("#FFFFFF");

      // Texto en blanco sobre la barra
      if (corte.razon_social || corte.rfc || corte.ubicacion) {
        doc
          .fillColor("#FFFFFF")
          .fontSize(8)
          .font("Helvetica");

        // Razón social (izquierda)
        if (corte.razon_social) {
          doc.text(
            corte.razon_social,
            50,
            footerY + 14,
            { width: 180, align: "left" }
          );
        }

        // RFC (centro)
        if (corte.rfc) {
          doc.text(
            `RFC: ${corte.rfc}`,
            230,
            footerY + 14,
            { width: 150, align: "center" }
          );
        }

        // Ubicación (derecha)
        if (corte.ubicacion) {
          doc.text(
            corte.ubicacion,
            380,
            footerY + 14,
            { width: 182, align: "right" }
          );
        }
      }

      doc.end();

    } catch (err) {
      reject(err);
    }
  });
}

module.exports = { generateCortePDF };



