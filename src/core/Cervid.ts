import fs from 'fs';
import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { type ColumnData, DataFrame } from './DataFrame.js';
import { readNDJSONNitro, scanNDJSON, type LazyNDJSON } from '../io/readNDJSON.js';
import { _readJSONOptimized } from '../io/readJSON.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export interface CervidReadOptions {
  type?: 'csv' | 'tsv' | 'json' | 'ndjson';
  mode?: 'eager' | 'stream';
  workers?: number;
  indexerCapacity?: number;
  useOffsets?: boolean;
  delimiter?: string;
  ndjson?: boolean;
  batchSize?: number;
  onBatch?: (df: DataFrame) => void | Promise<void>;
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
  return _readDelimited(filePath, {
    ...options,
    delimiter: options.delimiter ?? ',',
  });
}

async function _readTSV(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
  return _readDelimited(filePath, {
    ...options,
    delimiter: '\t',
  });
}

async function _readJSON(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
  if (options.ndjson === true || options.type === 'ndjson') {
    return readNDJSONNitro(filePath, options);
  }

  return _readJSONOptimized(filePath, {
    ...(options.workers !== undefined && { workers: options.workers }),
    ...(options.batchSize !== undefined && { batchSize: options.batchSize }),
  });
}

export const Cervid = {
  async read(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
    const ext = filePath.split('.').pop()?.toLowerCase();

    if (options.mode === 'stream') {
      if (!options.onBatch) {
        throw new Error(`Cervid.read(..., { mode: 'stream' }) requiere onBatch(df).`);
      }

      await this.streamJSON(filePath, {
        ...options,
        onBatch: options.onBatch,
      });
      return new DataFrame({
        columns: {},
        rowCount: 0,
        headers: [],
        originalBuffer: null,
        offsets: null,
        colMap: null,
        metadata: {
          mode: 'stream',
          source: filePath,
        },
      });
    }

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

  async streamJSON(
    filePath: string,
    options: CervidReadOptions & {
      onBatch: (df: DataFrame) => void | Promise<void>;
    },
  ): Promise<void> {
    await _readJSONOptimized(filePath, {
      ...(options.workers !== undefined && { workers: options.workers }),
      ...(options.batchSize !== undefined && { batchSize: options.batchSize }),
      onBatch: options.onBatch,
    });
  },

  scan(filePath: string, options: CervidReadOptions = {}): LazyNDJSON {
    return scanNDJSON(filePath, options);
  },
};
