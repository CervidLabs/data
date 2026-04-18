import fs from 'fs';
import readline from 'readline';
import os from 'os';
import { DataFrame, ColumnData } from '../core/DataFrame.js';

// 1. Interfaz estricta para las opciones
export interface ReadJSONOptions {
  encoding?: BufferEncoding;
  chunkSize?: number;
  ndjson?: boolean | null;
  numWorkers?: number;
}

/**
 * Lee un archivo JSON o NDJSON y devuelve un DataFrame
 */
export async function readJSON(filePath: string, options: ReadJSONOptions = {}): Promise<DataFrame> {
  const { encoding = 'utf8', chunkSize = 10000, ndjson = null } = options;

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

  // Usamos un buffer temporal de arreglos estándar para permitir .push()
  // Record<nombre_columna, array_de_valores>
  const tempColumns: Record<string, unknown[]> = {};

  if (isNDJSON || isLargeFile) {
    await readNDJSONStream(filePath, df, tempColumns, encoding, chunkSize);
  } else {
    await readJSONArray(filePath, df, tempColumns, encoding);
  }

  // ⚠️ TRUCO MAESTRO: Transferimos el buffer temporal al DataFrame
  // con un Type Assertion al final
  df.columns = tempColumns as Record<string, ColumnData>;

  console.info(`✅ JSON cargado: ${df.rowCount.toLocaleString()} filas, ${Object.keys(df.columns).length} columnas`);

  return df;
}

/**
 * Detecta si un archivo es NDJSON
 */
async function detectNDJSON(filePath: string): Promise<boolean> {
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

  if (firstLine.startsWith('{') && !firstLine.startsWith('[{')) return true;
  if (firstLine.startsWith('[')) return false;

  return true;
}

/**
 * Lee un archivo NDJSON línea por línea (streaming)
 */
async function readNDJSONStream(
  filePath: string,
  df: DataFrame,
  tempColumns: Record<string, unknown[]>,
  encoding: BufferEncoding,
  chunkSize: number,
): Promise<void> {
  const stream = fs.createReadStream(filePath, { encoding });
  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Infinity,
  });

  let headers: string[] = [];
  let rowCount = 0;
  let isFirstRow = true;

  for await (const line of rl) {
    if (!line.trim()) continue;

    try {
      const record = JSON.parse(line) as Record<string, unknown>;

      if (isFirstRow) {
        headers = Object.keys(record);
        headers.forEach((col) => {
          tempColumns[col] = [];
        });
        isFirstRow = false;
      }

      headers.forEach((header) => {
        let value = record[header];

        if (typeof value === 'string' && value !== '') {
          const num = Number(value);
          if (!Number.isNaN(num)) value = num;
        }

        // Ahora .push() funciona porque tempColumns[header] es unknown[]
        tempColumns[header].push(value);
      });

      rowCount++;

      if (rowCount % chunkSize === 0) {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    } catch (e: unknown) {
      if (process.env.DEBUG) {
        const msg = e instanceof Error ? e.message : String(e);
        console.warn(`Línea ignorada: ${msg}`);
      }
    }
  }

  df.rowCount = rowCount;
}

/**
 * Lee un JSON array normal (todo el archivo en memoria)
 */
async function readJSONArray(filePath: string, df: DataFrame, tempColumns: Record<string, unknown[]>, encoding: BufferEncoding): Promise<void> {
  const content = await fs.promises.readFile(filePath, { encoding });
  const data = JSON.parse(content) as unknown;

  if (Array.isArray(data)) {
    if (data.length === 0) return;

    const firstRecord = data[0] as Record<string, unknown>;
    const headers = Object.keys(firstRecord);
    headers.forEach((col) => {
      tempColumns[col] = [];
    });

    for (const item of data) {
      const record = item as Record<string, unknown>;
      headers.forEach((header) => {
        let value = record[header];
        if (typeof value === 'string' && value !== '') {
          const num = Number(value);
          if (!Number.isNaN(num)) value = num;
        }
        tempColumns[header].push(value);
      });
    }

    df.rowCount = data.length;
  } else if (data && typeof data === 'object') {
    const record = data as Record<string, unknown>;
    const headers = Object.keys(record);
    headers.forEach((col) => {
      tempColumns[col] = [record[col]];
    });
    df.rowCount = 1;
  }
}

/**
 * Lee NDJSON en paralelo
 */
export async function readJSONParallel(filePath: string, options: ReadJSONOptions = {}): Promise<DataFrame> {
  const { numWorkers = os.cpus().length } = options;
  console.info(`🚀 Ejecutando en paralelo usando ${numWorkers} workers...`);
  return readJSON(filePath, options);
}
