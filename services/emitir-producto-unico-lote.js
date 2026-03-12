'use strict';

// POST /recibos/producto-unico/lote

const pLimit = require('p-limit');
const crypto = require('crypto');
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ─── 1. VALIDACIÓN DE INPUT ──────────────────────────────────────────────────

function validateInput({ id_producto, id_lote, id_alumnos, id_plantel }) {
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
//
// REGLA: Esta función SIEMPRE se llama dentro de executeInTransaction().
// No ejecutar fuera de una transacción.

async function crearBorradoresTx(conn, { id_producto, id_lote, id_alumnos, id_usuario, id_plantel }) {

  // 3a. Validar y bloquear producto
  const [[producto]] = await conn.execute(
    `SELECT id_producto, frecuencia, status
     FROM productos
     WHERE id_producto = ?
     FOR UPDATE`,
    [id_producto]
  );
  validateProducto(producto, id_producto);

  // 3b. Obtener todos los alumnos en una sola query
  const placeholders = id_alumnos.map(() => '?').join(', ');
  const [alumnos] = await conn.execute(
    `SELECT id_alumno, id_plantel_academico, id_grupo
     FROM alumnos
     WHERE id_alumno IN (${placeholders})`,
    id_alumnos
  );

  // 3c. Verificar que todos los alumnos existen
  if (alumnos.length !== id_alumnos.length) {
    const encontrados = new Set(alumnos.map(a => a.id_alumno.toLowerCase()));
    const faltantes   = id_alumnos.filter(id => !encontrados.has(id.toLowerCase()));
    const err = new Error(`Alumnos no encontrados: ${faltantes.join(', ')}`);
    err.statusCode = 404;
    throw err;
  }

  // Normalizar claves a lowercase para que el Map sea case-insensitive.
  // AppSheet puede enviar UUIDs en uppercase; MySQL siempre devuelve lowercase.
  // Sin esto, alumnosMap.get(id_alumno) devuelve undefined → mysql2 omite
  // el valor en el array de parámetros → desplazamiento de columnas en el INSERT.
  const alumnosMap = new Map(alumnos.map(a => [a.id_alumno.toLowerCase(), a]));

  // 3d. Generar UUIDs para los recibos
  const recibos = id_alumnos.map(id_alumno => ({
    id_recibo : crypto.randomUUID(),
    id_alumno : id_alumno.toLowerCase(),                    // normalizar antes del INSERT
    alumno    : alumnosMap.get(id_alumno.toLowerCase()),
  }));

  // Guard: nunca debe llegar aquí con id_alumno inválido, pero si ocurre
  // cortamos antes del bulk INSERT para evitar el error de MySQL.
  const invalidos = recibos.filter(r => !r.id_alumno || !r.alumno);
  if (invalidos.length > 0) {
    const err = new Error(`id_alumno sin mapeo: ${invalidos.map(r => r.id_alumno).join(', ')}`);
    err.statusCode = 500;
    throw err;
  }

  // 3e. Bulk INSERT recibos
  //     Cada fila: (id_recibo, id_alumno, id_plantel, id_plantel_academico,
  //                  id_grupo, id_usuario, fecha, status_recibo, created_at, id_lote)
  const reciboRowPlaceholder = `(?, ?, ?, ?, ?, ?, CURDATE(), 'Borrador', NOW(), ?)`;
  const reciboValues = recibos.flatMap(({ id_recibo, id_alumno, alumno }) => [
    id_recibo,
    id_alumno,
    id_plantel,   // id_plantel  (plantel de cobro que viene en el body)
    id_plantel,   // id_plantel_academico
    alumno.id_grupo,
    id_usuario,
    id_lote,
  ]);

  await conn.execute(
    `INSERT INTO recibos (
       id_recibo, id_alumno, id_plantel, id_plantel_academico,
       id_grupo, id_usuario, fecha, status_recibo, created_at, id_lote
     ) VALUES ${recibos.map(() => reciboRowPlaceholder).join(', ')}`,
    reciboValues
  );

  // 3f. Bulk INSERT recibos_detalle
  //     Cada fila: (id_detalle, id_recibo, id_producto,
  //                  frecuencia_producto, cantidad, precio_base, status_detalle)
  const detalleRowPlaceholder = `(UUID(), ?, ?, 'Unica', 1, 0, 'Borrador')`;
  const detalleValues = recibos.flatMap(({ id_recibo }) => [id_recibo, id_producto]);

  await conn.execute(
    `INSERT INTO recibos_detalle (
       id_detalle, id_recibo, id_producto,
       frecuencia_producto, cantidad, precio_base, status_detalle
     ) VALUES ${recibos.map(() => detalleRowPlaceholder).join(', ')}`,
    detalleValues
  );

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
    // No propagar — registrar en log y seguir
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
//
// REGLA: emitirRecibo maneja su propia transacción interna.
//        Si falla un recibo → registrar error y continuar con los demás.

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
    let { id_producto, id_lote, id_alumnos, id_plantel } = req.body;

   // Convertir string de AppSheet a array
if (typeof id_alumnos === 'string') {
  id_alumnos = id_alumnos
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

// Validar input
try {
  validateInput({ id_producto, id_lote, id_alumnos, id_plantel });
} catch (err) {
  return res.status(err.statusCode || 400).json({ error: err.message });
}

    // Validar sesión
    if (!req.user?.id_usuario) {
      return res.status(401).json({ error: 'Usuario no autenticado' });
    }

    const id_usuario = req.user.id_usuario;

    console.log('[lote] Inicio', { lote_id: id_lote, producto: id_producto, cantidad_alumnos: id_alumnos.length });

    try {

      // ── FASE 1: Crear borradores en una única transacción ──────────────────
      const idsRecibos = await executeInTransaction(conn =>
        crearBorradoresTx(conn, { id_producto, id_lote, id_alumnos, id_usuario, id_plantel })
      );

      console.log(`[lote] Borradores creados — lote=${id_lote} cantidad=${idsRecibos.length}`);

      // ── FASE 2: Limpiar errores previos del lote ───────────────────────────
      await limpiarErroresPrevios(pool, id_lote);

      // ── FASE 3: Emitir recibos FUERA de la transacción ────────────────────
      console.log(`[lote] Iniciando emisión — lote=${id_lote}`);

      const { emitidos, fallidos } = await emitirRecibosLote(pool, emitirRecibo, idsRecibos);

      console.log('[lote] Finalizado', { lote_id: id_lote, producto: id_producto, cantidad_alumnos: id_alumnos.length, emitidos, fallidos });

      return res.json({
        ok              : true,
        id_lote,
        recibos_generados: idsRecibos.length,
        emitidos,
        fallidos,
      });

    } catch (err) {
      next(err);
    }
  };
};
