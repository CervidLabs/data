import fs from 'fs';
import readline from 'readline';
import { DataFrame } from '../core/DataFrame.js';

/**
 * Lee un archivo JSON o NDJSON y devuelve un DataFrame
 * @param {string} filePath - Ruta del archivo
 * @param {Object} options - Opciones de configuración
 * @param {string} options.encoding - Codificación (por defecto 'utf8')
 * @param {number} options.chunkSize - Tamaño del batch (por defecto 10000)
 * @param {boolean} options.ndjson - Si es NDJSON (auto-detecta)
 * @returns {Promise<DataFrame>}
 */
export async function readJSON(filePath, options = {}) {
  const {
    encoding = 'utf8',
    chunkSize = 10000,
    ndjson = null
  } = options;

  const df = new DataFrame();
  df.filePath = filePath;
  df.fileType = 'json';

  const stats = fs.statSync(filePath);
  const isLargeFile = stats.size > 100 * 1024 * 1024; // > 100MB

  // Detectar si es NDJSON
  let isNDJSON = ndjson;
  if (isNDJSON === null) {
    isNDJSON = await detectNDJSON(filePath);
  }

  if (isNDJSON || isLargeFile) {
    // NDJSON o archivo grande: streaming línea por línea
    await readNDJSONStream(filePath, df, encoding, chunkSize);
  } else {
    // JSON array normal: leer todo
    await readJSONArray(filePath, df, encoding);
  }

  console.log(`✅ JSON cargado: ${df.rowCount.toLocaleString()} filas, ${Object.keys(df.columns).length} columnas`);
  
  return df;
}

/**
 * Detecta si un archivo es NDJSON (una línea = un objeto)
 */
async function detectNDJSON(filePath) {
  const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  
  let firstLine = '';
  for await (const line of rl) {
    if (line.trim()) {
      firstLine = line.trim();
      break;
    }
  }
  stream.destroy();
  
  // Si la primera línea empieza con { y no es un array, probablemente es NDJSON
  if (firstLine.startsWith('{') && !firstLine.startsWith('[{')) {
    return true;
  }
  
  // Si empieza con [, es JSON array normal
  if (firstLine.startsWith('[')) {
    return false;
  }
  
  // Por defecto asumir NDJSON (más común para big data)
  return true;
}

/**
 * Lee un archivo NDJSON línea por línea (streaming)
 */
async function readNDJSONStream(filePath, df, encoding, chunkSize) {
  const stream = fs.createReadStream(filePath, { encoding });
  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  let headers = [];
  let rowCount = 0;
  let batch = [];
  let isFirstRow = true;

  for await (const line of rl) {
    if (!line.trim()) continue;

    try {
      const record = JSON.parse(line);
      
      // Primera fila: definir columnas
      if (isFirstRow) {
        headers = Object.keys(record);
        df.columns = {};
        headers.forEach(col => {
          df.columns[col] = [];
        });
        isFirstRow = false;
      }
      
      // Agregar valores a las columnas
      headers.forEach(header => {
        let value = record[header];
        
        // Convertir números si es posible
        if (value !== null && value !== undefined && typeof value === 'string') {
          const num = Number(value);
          if (!isNaN(num) && value.trim() !== '') {
            value = num;
          }
        }
        
        df.columns[header].push(value);
      });
      
      rowCount++;
      
      // Limpiar batch periódicamente
      if (batch.length >= chunkSize) {
        batch = [];
      }
      
    } catch (e) {
      // Ignorar líneas mal formadas
      if (process.env.DEBUG) {
        console.warn(`Línea ignorada: ${e.message}`);
      }
    }
  }

  df.rowCount = rowCount;
}

/**
 * Lee un JSON array normal (todo el archivo)
 */
async function readJSONArray(filePath, df, encoding) {
  const content = await fs.promises.readFile(filePath, { encoding });
  const data = JSON.parse(content);
  
  // Si es un array
  if (Array.isArray(data)) {
    if (data.length === 0) return;
    
    const headers = Object.keys(data[0]);
    df.columns = {};
    headers.forEach(col => {
      df.columns[col] = [];
    });
    
    for (const record of data) {
      headers.forEach(header => {
        let value = record[header];
        if (typeof value === 'string') {
          const num = Number(value);
          if (!isNaN(num) && value.trim() !== '') {
            value = num;
          }
        }
        df.columns[header].push(value);
      });
    }
    
    df.rowCount = data.length;
  } 
  // Si es un objeto único
  else if (typeof data === 'object' && data !== null) {
    const headers = Object.keys(data);
    df.columns = {};
    headers.forEach(col => {
      df.columns[col] = [data[col]];
    });
    df.rowCount = 1;
  }
}

/**
 * Lee NDJSON en paralelo (para archivos muy grandes)
 * @param {string} filePath 
 * @param {Object} options 
 * @returns {Promise<DataFrame>}
 */
export async function readJSONParallel(filePath, options = {}) {
  const { numWorkers = require('os').cpus().length } = options;
  
  // Por implementar: dividir archivo por bytes y usar Workers
  // Por ahora usamos la versión streaming normal
  return readJSON(filePath, options);
}