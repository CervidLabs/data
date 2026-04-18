import fs from 'fs';
import readline from 'readline';
import { DataFrame } from '../core/DataFrame.js';
import { logger } from '../lib/logger.js';

// 1. Definimos la interfaz estricta para las opciones
export interface ReadCSVOptions {
  delimiter?: string;
  header?: boolean;
  encoding?: BufferEncoding; // Usamos el tipo nativo de Node para encodings
  chunkSize?: number;
}

/**
 * Lee un archivo CSV y devuelve un DataFrame
 */
export async function readCSV(filePath: string, options: ReadCSVOptions = {}): Promise<DataFrame> {
  // Valores por defecto seguros
  const { delimiter = ',', header = true, encoding = 'utf8', chunkSize = 10000 } = options;

  const df = new DataFrame();
  df.filePath = filePath;
  df.fileType = 'csv';

  const stream = fs.createReadStream(filePath, { encoding });
  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Infinity,
  });

  let headers: string[] = [];
  let isFirstLine = true;
  let rowCount = 0;

  // Nota de Auditoría: En tu código original esta variable se vaciaba
  // pero nunca se le hacía un .push(). La he tipado, pero deberás
  // implementar la lógica de batching real si decides usarla.
  let batch: string[][] = [];

  for await (const line of rl) {
    if (!line.trim()) continue;

    const values = parseCSVLine(line, delimiter);
    const tempColumns: Record<string, (string | number)[]> = {};
    // Primera línea: headers
    if (isFirstLine && header) {
      headers = values;
      // 2. Inicializamos el buffer temporal, no el DataFrame directo
      headers.forEach((col) => {
        tempColumns[col] = [];
      });
      isFirstLine = false;
      continue;
    }

    // Sin header: usar índices automáticos
    if (isFirstLine && !header) {
      headers = values.map((_, i) => `column_${i}`);
      df.columns = {};
      headers.forEach((col) => {
        df.columns[col] = [];
      });
      isFirstLine = false;
    }

    // Procesar fila
    headers.forEach((headerKey, i) => {
      // Garantizamos que 'value' es string antes de procesar
      let value: string | number = values[i] || '';

      // Lógica estricta para parsear números en TS
      if (typeof value === 'string' && value !== '') {
        const lowerValue = value.toLowerCase();
        if (lowerValue !== 'null' && lowerValue !== 'undefined') {
          const num = Number(value);
          // Validamos que no sea NaN y que no sea un string de puros espacios
          if (!Number.isNaN(num) && value.trim() !== '') {
            value = num;
          }
        }
      }

      tempColumns[headerKey].push(value);
    });

    rowCount++;
    batch.push(values); // <- Corrección del bug original

    // Limpiar batch cada cierto tiempo
    if (batch.length >= chunkSize) {
      batch = [];
      // Aquí iría tu lógica futura de vaciado a memoria plana
    }
  }

  df.rowCount = rowCount;

  // ESLint suele quejarse de los console.log en producción.
  // En un motor serio, querrás cambiar esto por un logger interno.
  logger.info(`CSV cargado: ${rowCount.toLocaleString()} filas, ${headers.length} columnas`);
  return df;
}

/**
 * Parsea una línea de CSV respetando comillas
 */
function parseCSVLine(line: string, delimiter: string): string[] {
  const result: string[] = [];
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
 * Limpia el valor (remueve comillas al inicio/final y recorta espacios)
 */
function cleanValue(value: string): string {
  let cleaned = value.trim();
  if (cleaned.startsWith('"') && cleaned.endsWith('"')) {
    cleaned = cleaned.slice(1, -1);
  }
  return cleaned;
}
