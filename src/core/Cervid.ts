import fs from 'fs';
import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { type ColumnData, DataFrame } from './DataFrame.js';
import { readNDJSONNitro, scanNDJSON, type LazyNDJSON } from '../io/readNDJSON.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

type JSONValue = string | number | boolean | null | { [key: string]: JSONValue } | JSONValue[];

interface JSONObject {
  [key: string]: JSONValue;
}

export interface CervidReadOptions {
  type?: 'csv' | 'tsv' | 'json' | 'ndjson';
  workers?: number;
  indexerCapacity?: number;
  useOffsets?: boolean;
  delimiter?: string;
  ndjson?: boolean;
}

interface WorkerData {
  sharedBuffer: SharedArrayBuffer;
  offsetBuffer: SharedArrayBuffer | null;
  colBuffers: SharedArrayBuffer[];
  start: number;
  end: number;
  startRow: number;
  headers: string[];
  delimiter: string;
}

interface WorkerMessage {
  type: 'done';
  rowCount: number;
}

async function _readDelimited(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
  const delimiter = options.delimiter ?? ',';

  const stats = fs.statSync(filePath);
  const fileSize = stats.size;
  const fd = fs.openSync(filePath, 'r');

  const headerBuffer = Buffer.alloc(10000);
  fs.readSync(fd, headerBuffer, 0, 10000, 0);
  const firstLine = headerBuffer.toString().split('\n')[0];
  const headers = firstLine.trim().split(delimiter);
  const totalCols = headers.length;

  const numWorkers = options.workers ?? os.cpus().length;
  const capacity = options.indexerCapacity ?? 10_000_000;

  const sharedBuffer = new SharedArrayBuffer(fileSize);
  fs.readSync(fd, new Uint8Array(sharedBuffer), 0, fileSize, 0);
  fs.closeSync(fd);

  const colBuffers = headers.map(() => new SharedArrayBuffer(capacity * 8));
  const useOffsets = options.useOffsets !== false;
  const offsetBuffer = useOffsets ? new SharedArrayBuffer(capacity * totalCols * 2 * 4) : null;

  const chunkSize = Math.floor(fileSize / numWorkers);
  const promises: Promise<void>[] = [];
  let totalRowCount = 0;

  for (let i = 0; i < numWorkers; i++) {
    const start = i * chunkSize;
    const end = i === numWorkers - 1 ? fileSize : (i + 1) * chunkSize;

    promises.push(
      new Promise((resolve, reject) => {
        const workerPath = path.join(__dirname, '..', 'workers', 'ingest.worker.js');

        const workerData: WorkerData = {
          sharedBuffer,
          offsetBuffer,
          colBuffers,
          start,
          end,
          startRow: Math.floor(capacity / numWorkers) * i,
          headers,
          delimiter,
        };

        const worker = new Worker(workerPath, { workerData });

        worker.on('message', (msg: WorkerMessage) => {
          if (msg.type === 'done') {
            totalRowCount += msg.rowCount;
            resolve();
          }
        });

        worker.on('error', reject);
        worker.on('exit', (code) => {
          if (code !== 0) {
            reject(new Error(`Worker finalizó con código ${code}`));
          }
        });
      }),
    );
  }

  await Promise.all(promises);

  const columns: Record<string, ColumnData> = {};
  headers.forEach((h, i) => {
    columns[h] = new Float64Array(colBuffers[i]);
  });

  return new DataFrame({
    columns,
    rowCount: totalRowCount,
    headers,
    originalBuffer: sharedBuffer,
    offsets: offsetBuffer ? new Int32Array(offsetBuffer) : null,
    colMap: Object.fromEntries(headers.map((h, i) => [h, i])),
  });
}

async function _readCSV(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
  return _readDelimited(filePath, { ...options, delimiter: options.delimiter ?? ',' });
}

async function _readTSV(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
  return _readDelimited(filePath, { ...options, delimiter: '\t' });
}

function flattenObject(obj: JSONObject, prefix = ''): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      Object.assign(result, flattenObject(value as JSONObject, fullKey));
    } else {
      result[fullKey] = value;
    }
  }
  return result;
}

async function _readJSON(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
  const forceNDJSON = options.ndjson === true || options.type === 'ndjson';
  if (forceNDJSON) {
    return readNDJSONNitro(filePath, options);
  }

  const raw = fs.readFileSync(filePath, 'utf8');
  const dataRaw = JSON.parse(raw) as JSONValue;
  let data: JSONValue[];

  if (!Array.isArray(dataRaw)) {
    if (dataRaw !== null && typeof dataRaw === 'object') {
      const obj = dataRaw as JSONObject;
      const rootKey = Object.keys(obj).find((key) => Array.isArray(obj[key]));
      data = rootKey ? (obj[rootKey] as JSONValue[]) : [obj];
    } else {
      data = [dataRaw];
    }
  } else {
    data = dataRaw;
  }

  const flattenedData = data
    .filter((item): item is JSONObject => item !== null && typeof item === 'object' && !Array.isArray(item))
    .map((item) => flattenObject(item));

  const allKeys = new Set<string>();
  flattenedData.forEach((row) => {
    Object.keys(row).forEach((k) => allKeys.add(k));
  });

  const headers = Array.from(allKeys);
  const columns: Record<string, ColumnData> = {};
  headers.forEach((h) => {
    columns[h] = flattenedData.map((row) => row[h] ?? null);
  });

  return new DataFrame({
    columns,
    rowCount: flattenedData.length,
    headers,
    originalBuffer: null,
    offsets: null,
    colMap: Object.fromEntries(headers.map((h, i) => [h, i])),
  });
}

export const Cervid = {
  /**
   * Eagerly read any supported file format into a DataFrame.
   * Format is inferred from extension or `options.type`.
   */
  async read(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
    const ext = filePath.split('.').pop()?.toLowerCase();

    if (ext === 'ndjson' || options.type === 'ndjson') {
      return readNDJSONNitro(filePath, options);
    }
    if (ext === 'json' || options.type === 'json') {
      return _readJSON(filePath, options);
    }
    if (ext === 'tsv' || options.type === 'tsv') {
      return _readTSV(filePath, options);
    }

    return _readCSV(filePath, options);
  },

  /**
   * Lazily scan an NDJSON file without parsing any values.
   *
   * Returns a {@link LazyNDJSON} handle that lets you call `.select(['col1',
   * 'col2'])` to parse only the columns you need — workers skip everything
   * else, saving up to 80 % of CPU time on wide files.
   *
   * Schema discovery uses stochastic byte-level sampling (head + middle + tail)
   * so it runs in constant time regardless of file size.
   *
   * @example
   * // Inspect schema without reading any data
   * const lazy = await Cervid.scan('./events.ndjson');
   * console.log(lazy.schema.fields.map(f => f.name));
   *
   * // Parse only two columns
   * const df = await lazy.select(['user_id', 'score']);
   *
   * // Parse everything (equivalent to Cervid.read)
   * const full = await lazy.collect();
   */
  // Cervid.ts

  scan(filePath: string, options: CervidReadOptions = {}): LazyNDJSON {
    // Al no ser async, no hay error de 'require-await'
    return scanNDJSON(filePath, options);
  },
};
