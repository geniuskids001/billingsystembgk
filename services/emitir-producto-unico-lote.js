'use strict';

// POST /recibos/producto-unico/lote

const pLimit = require('p-limit');
const crypto = require('crypto');
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;



// ─── 1. VALIDACIÓN DE INPUT ──────────────────────────────────────────────────

function validateInput({ id_producto, id_lote, id_alumnos, id_plantel, forma_pago, fecha }) {
  if (!id_producto || typeof id_producto !== 'string' || !id_producto.trim()) {
    const err = new Error('id_producto es requerido');
    err.statusCode = 400;
    throw err;
  }
  if (!id_lote || !UUID_REGEX.test(id_lote)) {
    const err = new Error('id_lote debe ser un UUID válido');
    err.statusCode = 400;
    throw err;
  }
  if (!id_plantel || typeof id_plantel !== 'string') {
    const err = new Error('id_plantel requerido');
    err.statusCode = 400;
    throw err;
  }
  if (!Array.isArray(id_alumnos) || id_alumnos.length === 0) {
    const err = new Error('id_alumnos debe ser un array no vacío');
    err.statusCode = 400;
    throw err;
  }
  if (id_alumnos.length > 500) {
    const err = new Error('Lote demasiado grande: máximo 500 alumnos por lote');
    err.statusCode = 400;
    throw err;
  }
  if (!id_alumnos.every(id => typeof id === 'string' && id.trim())) {
    const err = new Error('Todos los elementos de id_alumnos deben ser strings válidos');
    err.statusCode = 400;
    throw err;
  }
  if (!forma_pago || typeof forma_pago !== 'string' || !forma_pago.trim()) {
    const err = new Error('forma_pago es requerido');
    err.statusCode = 400;
    throw err;
  }
  if (!fecha || typeof fecha !== 'string' || !fecha.trim()) {
    const err = new Error('fecha es requerida');
    err.statusCode = 400;
    throw err;
  }
}

// ─── 2. VALIDACIÓN DE PRODUCTO ───────────────────────────────────────────────

function validateProducto(producto, id_producto) {
  if (!producto) {
    const err = new Error(`Producto no encontrado: ${id_producto}`);
    err.statusCode = 404;
    throw err;
  }
  if (producto.frecuencia !== 'Unica') {
    const err = new Error(`El producto ${id_producto} debe tener frecuencia 'Unica'`);
    err.statusCode = 422;
    throw err;
  }
  if (producto.status !== 'Activo') {
    const err = new Error(`El producto ${id_producto} está inactivo`);
    err.statusCode = 422;
    throw err;
  }
}

// ─── 3. CREACIÓN DE BORRADORES (ejecuta dentro de la TX) ─────────────────────

async function crearBorradoresTx(conn, { id_producto, id_lote, id_alumnos, id_usuario, id_plantel, forma_pago, fecha }) {

  // 3a. Validar y bloquear producto
  const [[producto]] = await conn.execute(
    `SELECT id_producto, frecuencia, status, precio_base
     FROM productos
     WHERE id_producto = ?
     FOR UPDATE`,
    [id_producto]
  );
  validateProducto(producto, id_producto);

  console.log('[lote][debug] precio_base del producto:', producto.precio_base);

  // 3b. Obtener todos los alumnos
  console.log('[lote][debug] ids que iran al IN():', id_alumnos);

  const placeholders = id_alumnos.map(() => '?').join(', ');
  const [alumnos] = await conn.execute(
    `SELECT id_alumno, id_plantel_academico, id_grupo
     FROM alumnos
     WHERE id_alumno IN (${placeholders})`,
    id_alumnos
  );

  console.log('[lote][debug] alumnos encontrados en BD:', alumnos.map(a => a.id_alumno));
  console.log('[lote][debug] cantidad solicitados:', id_alumnos.length);
  console.log('[lote][debug] cantidad encontrados:', alumnos.length);

  // 3c. Verificar que todos los alumnos existen
  if (alumnos.length !== id_alumnos.length) {
    const encontrados = new Set(alumnos.map(a => a.id_alumno.toLowerCase()));
    const faltantes   = id_alumnos.filter(id => !encontrados.has(id.toLowerCase()));
    console.log('[lote][debug] faltantes:', faltantes);
    const err = new Error(`Alumnos no encontrados: ${faltantes.join(', ')}`);
    err.statusCode = 404;
    throw err;
  }

  const alumnosMap = new Map(alumnos.map(a => [a.id_alumno.toLowerCase(), a]));

  console.log('[lote][debug] id_alumnos antes de generar recibos:', id_alumnos);

  // 3d. Generar UUIDs para los recibos
  const recibos = id_alumnos.map(id_alumno => ({
    id_recibo : crypto.randomUUID(),
    id_alumno : id_alumno.toLowerCase(),
    alumno    : alumnosMap.get(id_alumno.toLowerCase()),
  }));

  console.log('[lote][debug] recibos generados:', recibos.map(r => ({
    id_recibo: r.id_recibo,
    id_alumno: r.id_alumno,
  })));

  // Guard
  const invalidos = recibos.filter(r => !r.id_alumno || !r.alumno);
  if (invalidos.length > 0) {
    const err = new Error(`id_alumno sin mapeo: ${invalidos.map(r => r.id_alumno).join(', ')}`);
    err.statusCode = 500;
    throw err;
  }

  // 3e. Bulk INSERT recibos
  //     created_at viene desde JS con timezone México — sin NOW() en el SQL
  

  const filas = recibos.map(({ id_recibo, id_alumno, alumno }) => ({
    id_recibo,
    id_alumno,
    id_plantel,
    id_plantel_academico : alumno.id_plantel_academico ?? null,
    id_grupo             : alumno.id_grupo ?? null,
    id_usuario,
    fecha,
    status_recibo        : 'Borrador',
    id_lote,
    forma_pago,
  }));

  // Orden fijo de columnas — sin dinámico
  const columnas = [
    'id_recibo',
    'id_alumno',
    'id_plantel',
    'id_plantel_academico',
    'id_grupo',
    'id_usuario',
    'fecha',
    'status_recibo',
    'id_lote',
    'forma_pago',
  ];

  // Guard: id_alumno nunca puede estar vacío antes del INSERT
  for (const f of filas) {
    if (!f.id_alumno) {
      throw new Error('id_alumno vacío antes del INSERT');
    }
  }

  const reciboRowPlaceholders = filas
    .map(() => `(${columnas.map(() => '?').join(', ')})`)
    .join(', ');

  const reciboValues = filas.flatMap(f => columnas.map(col => f[col] ?? null));

  // ── Debug SQL FINAL completo ────────────────────────────────────────────
  const sqlRecibos = `
INSERT INTO recibos (${columnas.join(', ')})
VALUES ${reciboRowPlaceholders}
`;
  console.log('[lote][debug] SQL FINAL:', sqlRecibos);
  console.log('[lote][debug] VALUES:', reciboValues);
  console.log('[lote][debug] total valores enviados:', reciboValues.length);
  console.log('[lote][debug] filas recibos:', filas.length);
  console.log('[lote][debug] valoresPorFila:', columnas.length);

  // Guard mismatch
  if (reciboValues.length !== filas.length * columnas.length) {
    console.error('[lote][error] mismatch valores INSERT recibos', {
      filas          : filas.length,
      valores        : reciboValues.length,
      esperado       : filas.length * columnas.length,
      valoresPorFila : columnas.length,
    });
    throw new Error('Mismatch entre columnas y valores en INSERT recibos');
  }

  await conn.execute(sqlRecibos, reciboValues);

  // 3f. Bulk INSERT recibos_detalle — todo con ? sin UUID() ni literales mezclados
  const filasDetalle = recibos.map(({ id_recibo, id_alumno }) => ({
  id_detalle: crypto.randomUUID(),
  id_recibo,
  id_alumno,
  id_producto,
  frecuencia_producto: 'Unica',
  cantidad: 1,
  precio_base: producto.precio_base,
  status_detalle: 'Borrador',
}));

  const columnasDetalle = [
    'id_detalle',
    'id_recibo',
    'id_producto',
    'frecuencia_producto',
    'cantidad',
    'precio_base',
    'status_detalle',
  ];

  const detalleRowPlaceholders = filasDetalle
    .map(() => `(${columnasDetalle.map(() => '?').join(', ')})`)
    .join(', ');

  const detalleValues = filasDetalle.flatMap(f =>
    columnasDetalle.map(col => f[col] ?? null)
  );

  const sqlDetalle = `
INSERT INTO recibos_detalle (${columnasDetalle.join(', ')})
VALUES ${detalleRowPlaceholders}
`;
  console.log('[lote][debug] SQL DETALLE:', sqlDetalle);
  console.log('[lote][debug] VALUES DETALLE:', detalleValues);

  await conn.execute(sqlDetalle, detalleValues);

  return recibos.map(r => r.id_recibo);
}

// ─── 4. REGISTRO DE ERROR POR RECIBO ─────────────────────────────────────────

async function registrarError(pool, id_recibo, error) {
  try {
    await pool.execute(
      `UPDATE recibos SET error_message = ? WHERE id_recibo = ?`,
      [error.message.substring(0, 255), id_recibo]
    );
  } catch (dbError) {
    console.error(`[lote] No se pudo guardar error_message para recibo ${id_recibo}:`, dbError.message);
  }
}

// ─── 5. LIMPIEZA DE ERRORES PREVIOS ──────────────────────────────────────────

async function limpiarErroresPrevios(pool, id_lote) {
  await pool.execute(
    `UPDATE recibos SET error_message = NULL WHERE id_lote = ?`,
    [id_lote]
  );
}

// ─── 6. EMISIÓN DE RECIBOS ───────────────────────────────────────────────────

async function emitirRecibosLote(pool, emitirRecibo, idsRecibos) {
  const limit = pLimit(5);

  let emitidos = 0;
  let fallidos = 0;

  const tareas = idsRecibos.map(id_recibo =>
    limit(async () => {
      try {
        await emitirRecibo(
          { body: { id_recibo } },
          { json: () => {} },
          () => {}
        );
        emitidos++;
      } catch (error) {
        console.error(`[lote] Error al emitir recibo ${id_recibo}:`, error.message);
        await registrarError(pool, id_recibo, error);
        fallidos++;
      }
    })
  );

  await Promise.all(tareas);

  return { emitidos, fallidos };
}

// ─── 7. HANDLER (FACTORY) ────────────────────────────────────────────────────

module.exports = function emitirProductoUnicoLoteFactory({ pool, executeInTransaction, emitirRecibo }) {

  return async function emitirProductoUnicoLote(req, res, next) {

    console.log('[lote][debug] req.body completo:', JSON.stringify(req.body, null, 2));
    console.log('[lote][debug] id_alumnos RAW:', req.body.id_alumnos);
    console.log('[lote][debug] typeof id_alumnos RAW:', typeof req.body.id_alumnos);
    console.log('[lote][debug] esArray RAW:', Array.isArray(req.body.id_alumnos));

    let { id_producto, id_lote, id_alumnos, id_plantel, id_usuario, forma_pago, fecha } = req.body;

    if (typeof id_alumnos === 'string') {
      console.log('[lote][debug] id_alumnos string original:', id_alumnos);
      console.log('[lote][debug] id_alumnos string length:', id_alumnos.length);

      const parsed = id_alumnos.split(',');
      console.log('[lote][debug] id_alumnos despues split:', parsed);
      console.log('[lote][debug] cantidad despues split:', parsed.length);

      const parsedTrim = parsed.map(s => s.trim());
      console.log('[lote][debug] id_alumnos despues trim:', parsedTrim);

      id_alumnos = parsedTrim.filter(Boolean);
      console.log('[lote][debug] id_alumnos final:', id_alumnos);
      console.log('[lote][debug] cantidad final:', id_alumnos.length);
      console.log('[lote][debug] ids vacios detectados:', parsedTrim.filter(x => !x));

    } else if (Array.isArray(id_alumnos)) {
      console.log('[lote][debug] id_alumnos llego como array:', id_alumnos);
    }

    try {
      validateInput({ id_producto, id_lote, id_alumnos, id_plantel, forma_pago, fecha });
    } catch (err) {
      return res.status(err.statusCode || 400).json({ error: err.message });
    }

    if (!id_usuario) {
      return res.status(400).json({ error: 'id_usuario requerido' });
    }

    console.log('[lote] Inicio', {
      lote_id          : id_lote,
      producto         : id_producto,
      usuario          : id_usuario,
      plantel          : id_plantel,
      forma_pago,
      fecha,
      cantidad_alumnos : id_alumnos.length,
    });

    try {

      const [[loteExistente]] = await pool.execute(
        `SELECT 1 FROM recibos WHERE id_lote = ? LIMIT 1`,
        [id_lote]
      );
      if (loteExistente) {
        return res.status(409).json({ error: 'El id_lote ya existe en recibos' });
      }

      const idsRecibos = await executeInTransaction(conn =>
        crearBorradoresTx(conn, { id_producto, id_lote, id_alumnos, id_usuario, id_plantel, forma_pago, fecha })
      );

      console.log(`[lote] Borradores creados — lote=${id_lote} cantidad=${idsRecibos.length}`);

      await limpiarErroresPrevios(pool, id_lote);

      console.log(`[lote] Iniciando emisión — lote=${id_lote}`);

      const { emitidos, fallidos } = await emitirRecibosLote(pool, emitirRecibo, idsRecibos);

      console.log('[lote] Finalizado', {
        lote_id          : id_lote,
        producto         : id_producto,
        cantidad_alumnos : id_alumnos.length,
        emitidos,
        fallidos,
      });

      return res.json({
        ok               : true,
        id_lote,
        recibos_generados: idsRecibos.length,
        emitidos,
        fallidos,
      });

    } catch (err) {
      console.error('[lote][SQL ERROR]', {
  message: err.message,
  code: err.code,
  errno: err.errno,
  sqlMessage: err.sqlMessage,
  sqlState: err.sqlState,
  sql: err.sql,
  parameters: err.parameters,
  stack: err.stack
});
      next(err);
    }
  };
};