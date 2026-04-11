import fs from 'fs';
import readline from 'readline';
import { DataFrame } from '../core/DataFrame.js';

/**
 * Lee un archivo CSV y devuelve un DataFrame
 * @param {string} filePath - Ruta del archivo CSV
 * @param {Object} options - Opciones de configuración
 * @param {string} options.delimiter - Delimitador de columnas (por defecto ',')
 * @param {boolean} options.header - Si la primera fila es header (por defecto true)
 * @param {string} options.encoding - Codificación del archivo (por defecto 'utf8')
 * @param {number} options.chunkSize - Tamaño del batch para procesamiento (por defecto 10000)
 * @returns {Promise<DataFrame>}
 */
export async function readCSV(filePath, options = {}) {
  const {
    delimiter = ',',
    header = true,
    encoding = 'utf8',
    chunkSize = 10000
  } = options;

  const df = new DataFrame();
  df.filePath = filePath;
  df.fileType = 'csv';
  
  const stream = fs.createReadStream(filePath, { encoding });
  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  let headers = [];
  let isFirstLine = true;
  let rowCount = 0;
  let batch = [];

  for await (const line of rl) {
    if (!line.trim()) continue;

    const values = parseCSVLine(line, delimiter);

    // Primera línea: headers
    if (isFirstLine && header) {
      headers = values;
      df.columns = {};
      headers.forEach(col => {
        df.columns[col] = [];
      });
      isFirstLine = false;
      continue;
    }

    // Sin header: usar índices
    if (isFirstLine && !header) {
      headers = values.map((_, i) => `column_${i}`);
      df.columns = {};
      headers.forEach(col => {
        df.columns[col] = [];
      });
      isFirstLine = false;
    }

    // Procesar fila
    headers.forEach((header, i) => {
      let value = values[i] || '';
      
      // Intentar parsear números
      if (value !== '' && !isNaN(value) && value !== 'null' && value !== 'undefined') {
        const num = Number(value);
        if (!isNaN(num) && value.trim() !== '') {
          value = num;
        }
      }
      
      df.columns[header].push(value);
    });

    rowCount++;

    // Limpiar batch cada cierto tiempo (liberar memoria)
    if (batch.length >= chunkSize) {
      batch = [];
    }
  }

  df.rowCount = rowCount;
  console.log(`✅ CSV cargado: ${rowCount.toLocaleString()} filas, ${headers.length} columnas`);
  
  return df;
}

/**
 * Parsea una línea de CSV respetando comillas
 * @param {string} line 
 * @param {string} delimiter 
 * @returns {string[]}
 */
function parseCSVLine(line, delimiter) {
  const result = [];
  let current = '';
  let inQuotes = false;
  let i = 0;
  
  while (i < line.length) {
    const char = line[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
      i++;
      continue;
    }
    
    if (char === delimiter && !inQuotes) {
      result.push(cleanValue(current));
      current = '';
      i++;
      continue;
    }
    
    current += char;
    i++;
  }
  
  result.push(cleanValue(current));
  return result;
}

/**
 * Limpia el valor (remueve comillas al inicio/final)
 * @param {string} value 
 * @returns {string}
 */
function cleanValue(value) {
  value = value.trim();
  if (value.startsWith('"') && value.endsWith('"')) {
    value = value.slice(1, -1);
  }
  return value;
}