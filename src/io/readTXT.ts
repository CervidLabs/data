import fs from 'fs';
import readline from 'readline';
import { DataFrame, ColumnData } from '../core/DataFrame.js';

export interface ReadTXTOptions {
  delimiter?: string;
  header?: boolean;
  encoding?: BufferEncoding;
  chunkSize?: number;
  fixedWidths?: number[] | null;
  columnNames?: string[] | null;
}

interface DelimitedOptions {
  delimiter: string;
  header: boolean;
  encoding: BufferEncoding;
  chunkSize: number;
}

interface FixedWidthOptions {
  fixedWidths: number[];
  columnNames: string[] | null;
  encoding: BufferEncoding;
  chunkSize: number;
}

/**
 * Lee un archivo TXT delimitado o de ancho fijo y devuelve un DataFrame
 */
export async function readTXT(filePath: string, options: ReadTXTOptions = {}): Promise<DataFrame> {
  const { delimiter = '\t', header = true, encoding = 'utf8', chunkSize = 10000, fixedWidths = null, columnNames = null } = options;

  const df = new DataFrame();
  df.filePath = filePath;
  df.fileType = 'txt';

  // 1. Buffer temporal para evitar el error de .push()
  const tempColumns: Record<string, unknown[]> = {};

  if (fixedWidths) {
    await readFixedWidth(filePath, df, tempColumns, {
      fixedWidths,
      columnNames,
      encoding,
      chunkSize,
    });
  } else {
    await readDelimited(filePath, df, tempColumns, {
      delimiter,
      header,
      encoding,
      chunkSize,
    });
  }

  // 2. Transferencia final de memoria al DataFrame
  df.columns = tempColumns as Record<string, ColumnData>;

  console.info(`✅ TXT cargado: ${df.rowCount.toLocaleString()} filas, ${Object.keys(df.columns).length} columnas`);

  return df;
}

async function readDelimited(filePath: string, df: DataFrame, tempColumns: Record<string, unknown[]>, options: DelimitedOptions): Promise<void> {
  const { delimiter, header, encoding, chunkSize } = options;
  const stream = fs.createReadStream(filePath, { encoding });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers: string[] = [];
  let rowCount = 0;
  let isFirstLine = true;

  for await (const line of rl) {
    if (!line.trim()) continue;

    const values = parseDelimitedLine(line, delimiter);

    if (isFirstLine) {
      if (header) {
        headers = values.map((v) => cleanValue(v).replace(/[^a-zA-Z0-9_]/g, '_'));
        isFirstLine = false;
        headers.forEach((col) => {
          tempColumns[col] = [];
        });
        continue;
      } else {
        headers = values.map((_, i) => `col_${i}`);
        headers.forEach((col) => {
          tempColumns[col] = [];
        });
        isFirstLine = false;
      }
    }

    headers.forEach((headerKey, i) => {
      let value: string | number = values[i] || '';
      value = cleanValue(value as string);

      if (typeof value === 'string' && value !== '') {
        const num = Number(value);
        if (!Number.isNaN(num)) value = num;
      }

      // El push ahora es seguro sobre tempColumns
      tempColumns[headerKey].push(value);
    });

    rowCount++;
    if (rowCount % chunkSize === 0) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
  }

  df.rowCount = rowCount;
}

async function readFixedWidth(filePath: string, df: DataFrame, tempColumns: Record<string, unknown[]>, options: FixedWidthOptions): Promise<void> {
  const { fixedWidths, columnNames, encoding, chunkSize } = options;
  const stream = fs.createReadStream(filePath, { encoding });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  const headers = columnNames || fixedWidths.map((_, i) => `col_${i}`);
  headers.forEach((col) => {
    tempColumns[col] = [];
  });

  let rowCount = 0;
  let isFirstLine = true;
  const skipHeader = !columnNames;

  for await (const line of rl) {
    if (!line.trim()) continue;

    if (skipHeader && isFirstLine) {
      isFirstLine = false;
      continue;
    }

    let start = 0;
    headers.forEach((headerKey, i) => {
      const width = fixedWidths[i];
      let value: string | number = line.substring(start, start + width).trim();

      if (value !== '') {
        const num = Number(value);
        if (!Number.isNaN(num)) value = num;
      }

      tempColumns[headerKey].push(value);
      start += width;
    });

    rowCount++;
    if (rowCount % chunkSize === 0) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
  }

  df.rowCount = rowCount;
}

function parseDelimitedLine(line: string, delimiter: string): string[] {
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
      result.push(current);
      current = '';
      i++;
      continue;
    }
    current += char;
    i++;
  }
  result.push(current);
  return result;
}

function cleanValue(value: string): string {
  let cleaned = value.trim();
  if (cleaned.startsWith('"') && cleaned.endsWith('"')) {
    cleaned = cleaned.slice(1, -1);
  }
  return cleaned;
}
