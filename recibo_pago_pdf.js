const PDFDocument = require("pdfkit");
const path = require("path");

async function generateReciboPDF(recibo, detalles) {
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
      const HIGHLIGHT_BG = "#E8F4F8"; // Color suave para resaltar total
      const logoPath = path.join(__dirname, "assets/businesslogo.png");

  /* ================= HEADER ================= */
doc.image(logoPath, 50, 50, { width: 70 });

doc
  .fillColor(COLOR)
  .fontSize(22)
  .font("Helvetica-Bold")
  .text("RECIBO DE PAGO", 200, 50, { align: "right" });

// Validación de fecha
if (!recibo.fecha_emision) {
  throw new Error("Recibo sin fecha de emisión - datos inconsistentes");
}

const fechaEmision = new Intl.DateTimeFormat("es-MX", {
  timeZone: "America/Mexico_City",
  weekday: "long",
  year: "numeric",
  month: "long",
  day: "numeric"
}).format(new Date(recibo.fecha_emision));

const fechaEmisionFormateada =
  fechaEmision.charAt(0).toUpperCase() + fechaEmision.slice(1);

doc
  .fillColor(GRAY)
  .fontSize(9)
  .font("Helvetica")
  .text(`Folio: ${recibo.id_recibo || 'N/A'}`, 200, 80, { align: "right" })
  .text(`Forma de pago: ${recibo.forma_pago || 'N/A'}`, 200, 93, {
    align: "right"
  })
  .text(`Plantel: ${recibo.plantel_nombre || 'N/A'}`, 200, 106, {
    align: "right"
  });

doc
  .moveTo(50, 135)
  .lineTo(562, 135)
  .lineWidth(1.5)
  .stroke(COLOR);

/* ================= FECHA DESTACADA ================= */
doc
  .fillColor("#333333")
  .fontSize(9)
  .font("Helvetica")
  .text("FECHA DE EMISIÓN", 50, 155, {
    width: 512,
    align: "right"
  });

doc
  .fontSize(11)
  .font("Helvetica-Bold")
  .text(fechaEmisionFormateada, 50, 170, {
    width: 512,
    align: "right"
  });


/* ================= ALUMNO ================= */
doc
  .fillColor("#333333")
  .fontSize(9)
  .font("Helvetica")
  .text("DATOS DEL ALUMNO", 50, 205);   // ← antes 200

const MAX_CHARS_NOMBRE = 80;
const nombreAlumno = recibo.alumno_nombre_completo || 'Sin nombre';
const nombreSeguro = nombreAlumno.length > MAX_CHARS_NOMBRE
  ? nombreAlumno.slice(0, MAX_CHARS_NOMBRE - 3) + "..."
  : nombreAlumno;

doc
  .fontSize(11)
  .font("Helvetica-Bold")
  .text(nombreSeguro, 50, 220, { width: 512 });   // ← antes 215


/* ================= CONCEPTOS - TABLA CENTRADA ================= */
doc
  .fillColor("#333333")
  .fontSize(9)
  .font("Helvetica")
  .text("CONCEPTOS", 50, 250);   


      // Tabla centrada: empieza en 85 en lugar de 50 (35px de margen extra)
      const tableLeft = 85;
      const tableWidth = 477; // Ancho reducido para centrar
      const tableTop = 270;

      // Header de tabla
      doc.rect(tableLeft, tableTop, tableWidth, 25).fill(LIGHT_GRAY);

      doc
        .fillColor(COLOR)
        .fontSize(9)
        .font("Helvetica-Bold")
        .text("CONCEPTO", tableLeft + 10, tableTop + 8, { width: 180 })
        .text("PRECIO", tableLeft + 195, tableTop + 8, { width: 50, align: "right" })
        .text("BECA", tableLeft + 250, tableTop + 8, { width: 45, align: "right" })
        .text("DESC.", tableLeft + 300, tableTop + 8, { width: 50, align: "right" })
        .text("RECARGO", tableLeft + 355, tableTop + 8, { width: 55, align: "right" })
        .text("TOTAL", tableLeft + 415, tableTop + 8, { width: 52, align: "right" });

      let y = tableTop + 35;
      doc.fontSize(9).font("Helvetica");

      if (!detalles || detalles.length === 0) {
        doc
          .fillColor(GRAY)
          .fontSize(10)
          .text("Sin conceptos registrados", tableLeft, y, {
            width: tableWidth,
            align: "center"
          });
        y += 30;
      } else {
        const MAX_CHARS_DESC = 35;
        
        detalles.forEach((d, index) => {
          // Fondo zebra
          if (index % 2 === 0) {
            doc.rect(tableLeft, y - 5, tableWidth, 20).fill("#FAFBFC");
          }

          const descripcionRaw = d.descripcion || 'Sin descripción';
          const descripcion = descripcionRaw.length > MAX_CHARS_DESC
            ? descripcionRaw.slice(0, MAX_CHARS_DESC - 3) + "..."
            : descripcionRaw;

          const precioBase = Number(d.precio_base) || 0;
          const beca = Number(d.beca) || 0;
          const descuentoBase = Number(d.descuento) || 0;
const recargoBase = Number(d.recargo) || 0;
const montoAjuste = Number(d.monto_ajuste) || 0;

// Mezclar ajuste SOLO para visualización
let descuento = descuentoBase;
let recargo = recargoBase;

if (montoAjuste < 0) {
  descuento += Math.abs(montoAjuste);
} else if (montoAjuste > 0) {
  recargo += montoAjuste;
}

          const precioFinal = Number(d.precio_final) || 0;

   
          doc
            .fillColor("#333333")
            .text(descripcion, tableLeft + 10, y, { width: 180 })
            .text(`$${precioBase.toFixed(2)}`, tableLeft + 195, y, {
              width: 50,
              align: "right"
            })
            .text(`$${beca.toFixed(2)}`, tableLeft + 250, y, {
              width: 45,
              align: "right"
            })
            .text(`$${descuento.toFixed(2)}`, tableLeft + 300, y, {
              width: 50,
              align: "right"
            })
            .text(`$${recargo.toFixed(2)}`, tableLeft + 355, y, {
              width: 55,
              align: "right"
            });

          // TOTAL con negrita y color
          doc
            .fillColor(COLOR)
            .font("Helvetica-Bold")
            .text(`$${precioFinal.toFixed(2)}`, tableLeft + 415, y, {
              width: 52,
              align: "right"
            });

          // Restaurar fuente para siguiente fila
          doc.font("Helvetica").fillColor("#333333");

          y += 20;
        });
      }

      // Línea divisoria después de conceptos
      y += 10;
      doc
        .moveTo(tableLeft, y)
        .lineTo(tableLeft + tableWidth, y)
        .lineWidth(0.5)
        .stroke(BORDER_GRAY);

      /* ================= TOTAL - ALINEADO A LA DERECHA ================= */
      y += 25;

      const totalBoxWidth = 212;
      const totalBoxLeft = 562 - totalBoxWidth; // Alineado a la derecha

      doc
        .rect(totalBoxLeft, y, totalBoxWidth, 55)
        .lineWidth(2)
        .fillAndStroke(LIGHT_GRAY, COLOR);

      doc
        .fillColor(GRAY)
        .fontSize(10)
        .font("Helvetica")
        .text("TOTAL PAGADO", totalBoxLeft + 10, y + 12, { width: 192 });

      const totalRecibo = Number(recibo.total_recibo) || 0;
      
      doc
        .fillColor(COLOR)
        .fontSize(22)
        .font("Helvetica-Bold")
        .text(
          `$${totalRecibo.toFixed(2)}`,
          totalBoxLeft + 10,
          y + 28,
          {
            width: 192,
            align: "right"
          }
        );

      /* ================= FOOTER ================= */
      doc
        .fillColor(GRAY)
        .fontSize(8)
        .font("Helvetica")
        .text(
          "Este documento es un comprobante de pago válido.",
          50,
          690,
          { 
            align: "center", 
            width: 512 
          }
        );


/* ================= FOOTER PROFESIONAL ================= */
const footerY = 750; // Posición fija en la parte inferior

// Barra azul de fondo (siempre se dibuja)
doc
  .rect(0, footerY, doc.page.width, 42)
  .fill(COLOR);

// Línea decorativa superior
doc
  .moveTo(0, footerY)
  .lineTo(doc.page.width, footerY)
  .lineWidth(1)
  .stroke("#FFFFFF");

// Texto en blanco sobre la barra (solo si hay datos)
if (recibo.razon_social || recibo.rfc || recibo.ubicacion) {
  doc
    .fillColor("#FFFFFF")
    .fontSize(8)
    .font("Helvetica");

  // Razón social (izquierda)
  if (recibo.razon_social) {
    doc.text(
      recibo.razon_social,
      50,
      footerY + 14,
      { width: 180, align: "left" }
    );
  }

  // RFC (centro)
  if (recibo.rfc) {
    doc.text(
      `RFC: ${recibo.rfc}`,
      230,
      footerY + 14,
      { width: 150, align: "center" }
    );
  }

  // Ubicación (derecha)
  if (recibo.ubicacion) {
    doc.text(
      recibo.ubicacion,
      380,
      footerY + 14,
      { width: 182, align: "right" }
    );
  }
}

/* ============ WATERMARK PREMIUM CANCELADO ============ */
if (recibo.status_recibo === "Cancelado") {
  doc.save();

  const centerX = doc.page.width / 2;
  const centerY = doc.page.height / 2;

  // Configuración base: rojo semi-transparente
  doc.opacity(0.12).strokeColor("#CC0000").fillColor("#CC0000");

  // Círculo exterior (sello institucional)
  doc.lineWidth(8).circle(centerX, centerY, 230).stroke();

  // Círculo interior (marco interno)
  doc.lineWidth(3).circle(centerX, centerY, 195).stroke();

  // Aplicar rotación diagonal
  doc.rotate(-35, { origin: [centerX, centerY] });

  // Texto principal: CANCELADO
  doc.font("Helvetica-Bold").fontSize(72);
  const canceladoWidth = doc.widthOfString("CANCELADO");
  doc.text("CANCELADO", centerX - canceladoWidth / 2, centerY - 50, {
    lineBreak: false
  });

  // Texto institucional: BGK SISTEMA
  doc.fontSize(20);
  const sistemaWidth = doc.widthOfString("BGK SISTEMA");
  doc.text("BGK SISTEMA", centerX - sistemaWidth / 2, centerY + 65, {
    lineBreak: false
  });

  // Restaurar estado del documento
  doc.restore();
  doc.opacity(1);
}

doc.end();

} catch (err) {
  reject(err);
}
});
} 

module.exports = { generateReciboPDF };