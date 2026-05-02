/**
 * readJSON.ts — Streaming JSON reader. Zero-dep, bounded memory.
 *
 * Estrategia:
 *  - Lee el archivo en chunks.
 *  - Extrae objetos JSON completos aunque crucen boundaries.
 *  - Usa worker pool persistente.
 *  - Soporta modo eager: acumula todo y devuelve DataFrame.
 *  - Soporta modo stream: entrega cada batch a onBatch(df) y no acumula todo.
 */

import fs from 'fs';
import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { type ColumnData, DataFrame } from '../core/DataFrame.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── Tunables ──────────────────────────────────────────────────────────────

const CHUNK_SIZE = 64 * 1024 * 1024;
const DEFAULT_BATCH_RECORDS = 5_000;

// ─── Byte constants ────────────────────────────────────────────────────────

const B_OPEN_BRACE = 123;
const B_CLOSE_BRACE = 125;
const B_OPEN_BRACKET = 91;
const B_CLOSE_BRACKET = 93;
const B_QUOTE = 34;
const B_BACKSLASH = 92;
const B_COMMA = 44;
const B_SPACE = 32;
const B_LF = 10;
const B_CR = 13;
const B_TAB = 9;

function isWS(b: number): boolean {
  return b === B_SPACE || b === B_LF || b === B_CR || b === B_TAB;
}

// ─── Boundary extractor ────────────────────────────────────────────────────

function extractRecords(buf: Buffer): { records: string[]; consumedTo: number } {
  const records: string[] = [];
  const len = buf.length;
  let pos = 0;
  let consumedTo = 0;

  while (pos < len && buf[pos] !== B_OPEN_BRACE) {
    pos++;
  }

  while (pos < len) {
    if (buf[pos] !== B_OPEN_BRACE) {
      pos++;
      continue;
    }

    const objStart = pos;
    let depth = 0;
    let inStr = false;
    let complete = false;

    while (pos < len) {
      const b = buf[pos];

      if (inStr) {
        if (b === B_BACKSLASH) {
          pos += 2;
          continue;
        }

        if (b === B_QUOTE) {
          inStr = false;
        }

        pos++;
        continue;
      }

      switch (b) {
        case B_QUOTE:
          inStr = true;
          pos++;
          break;

        case B_OPEN_BRACE:
        case B_OPEN_BRACKET:
          depth++;
          pos++;
          break;

        case B_CLOSE_BRACE:
        case B_CLOSE_BRACKET:
          depth--;
          pos++;

          if (depth === 0) {
            records.push(buf.subarray(objStart, pos).toString('utf8'));
            consumedTo = pos;

            while (pos < len && (isWS(buf[pos]) || buf[pos] === B_COMMA)) {
              pos++;
            }

            complete = true;
          }
          break;

        default:
          pos++;
      }

      if (complete) {
        break;
      }
    }

    if (!complete) {
      break;
    }
  }

  return { records, consumedTo };
}

// ─── Flatten iterativo ─────────────────────────────────────────────────────

function flattenIterative(obj: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const stack: Array<[Record<string, unknown>, string]> = [[obj, '']];

  while (stack.length > 0) {
    const [cur, pre] = stack.pop()!;

    for (const key in cur) {
      const val = cur[key];
      const fullKey = pre ? `${pre}.${key}` : key;

      if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
        stack.push([val as Record<string, unknown>, fullKey]);
      } else {
        result[fullKey] = val;
      }
    }
  }

  return result;
}

// ─── Type detection ────────────────────────────────────────────────────────

function detectSchema(sampleRecords: string[]): {
  headers: string[];
  numericCols: Set<string>;
} {
  const keySet = new Set<string>();
  const flattened: Record<string, unknown>[] = [];

  for (const r of sampleRecords) {
    const flat = flattenIterative(JSON.parse(r) as Record<string, unknown>);
    flattened.push(flat);

    for (const k in flat) {
      keySet.add(k);
    }
  }

  const headers = Array.from(keySet);
  const numeric = new Set<string>(headers);

  for (const row of flattened) {
    for (const h of [...numeric]) {
      const v = row[h];

      if (v !== null && v !== undefined && typeof v !== 'number') {
        numeric.delete(h);
      }
    }

    if (numeric.size === 0) {
      break;
    }
  }

  return { headers, numericCols: numeric };
}

// ─── Public types ──────────────────────────────────────────────────────────

export interface BatchResult {
  numericData: Record<string, number[]>;
  stringData: Record<string, (string | null)[]>;
}

export interface JSONWorkerInit {
  headers: string[];
  numericHeaders: string[];
  stringHeaders: string[];
}

export interface JSONReadOptions {
  workers?: number | undefined;
  batchSize?: number | undefined;
  onBatch?: ((df: DataFrame) => void | Promise<void>) | undefined;
}

// ─── Worker pool ───────────────────────────────────────────────────────────

class WorkerPool {
  private idle: Worker[] = [];

  private pending: Array<{
    batch: string[];
    resolve: (r: BatchResult) => void;
    reject: (e: Error) => void;
  }> = [];

  constructor(private workers: Worker[]) {
    for (const w of workers) {
      w.on('message', (result: BatchResult) => this.onResult(w, result));

      w.on('error', (err) => {
        const reject = (w as unknown as Record<string, unknown>)['__reject'] as ((e: Error) => void) | undefined;

        if (reject) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      });

      this.idle.push(w);
    }
  }

  async dispatch(batch: string[]): Promise<BatchResult> {
    return new Promise((resolve, reject) => {
      const w = this.idle.pop();

      if (w) {
        this.send(w, batch, resolve, reject);
      } else {
        this.pending.push({ batch, resolve, reject });
      }
    });
  }

  private send(w: Worker, batch: string[], resolve: (r: BatchResult) => void, reject: (e: Error) => void): void {
    (w as unknown as Record<string, unknown>)['__resolve'] = resolve;
    (w as unknown as Record<string, unknown>)['__reject'] = reject;

    w.postMessage(batch);
  }

  private onResult(w: Worker, result: BatchResult): void {
    const resolve = (w as unknown as Record<string, unknown>)['__resolve'] as ((r: BatchResult) => void) | undefined;

    if (resolve) {
      resolve(result);
    }

    const next = this.pending.shift();

    if (next) {
      this.send(w, next.batch, next.resolve, next.reject);
    } else {
      this.idle.push(w);
    }
  }

  get idleCount(): number {
    return this.idle.length;
  }

  async terminate(): Promise<void> {
    await Promise.all(this.workers.map(async (w) => w.terminate()));
  }
}

// ─── Result accumulator para modo eager ────────────────────────────────────

class ColumnAccumulator {
  private numericChunks: Record<string, number[][]> = {};
  private stringChunks: Record<string, (string | null)[][]> = {};
  public totalRows = 0;

  constructor(
    private numericHeaders: string[],
    private stringHeaders: string[],
  ) {
    for (const h of numericHeaders) {
      this.numericChunks[h] = [];
    }

    for (const h of stringHeaders) {
      this.stringChunks[h] = [];
    }
  }

  merge(result: BatchResult, rowCount: number): void {
    for (const h of this.numericHeaders) {
      if (result.numericData[h]) {
        this.numericChunks[h].push(result.numericData[h]);
      }
    }

    for (const h of this.stringHeaders) {
      if (result.stringData[h]) {
        this.stringChunks[h].push(result.stringData[h]);
      }
    }

    this.totalRows += rowCount;
  }

  build(): Record<string, ColumnData> {
    const columns: Record<string, ColumnData> = {};

    for (const h of this.numericHeaders) {
      const chunks = this.numericChunks[h];
      const total = chunks.reduce((s, c) => s + c.length, 0);
      const f64 = new Float64Array(total);

      let offset = 0;

      for (const chunk of chunks) {
        f64.set(chunk, offset);
        offset += chunk.length;
      }

      columns[h] = f64;
    }

    for (const h of this.stringHeaders) {
      const merged: (string | null)[] = [];

      for (const chunk of this.stringChunks[h]) {
        for (let i = 0; i < chunk.length; i++) {
          merged.push(chunk[i]);
        }
      }

      columns[h] = merged;
    }

    return columns;
  }
}

// ─── Batch builder para modo stream ────────────────────────────────────────

function buildBatchColumns(result: BatchResult): Record<string, ColumnData> {
  const columns: Record<string, ColumnData> = {};

  for (const [h, values] of Object.entries(result.numericData)) {
    const arr = new Float64Array(values.length);

    for (let i = 0; i < values.length; i++) {
      arr[i] = values[i];
    }

    columns[h] = arr;
  }

  for (const [h, values] of Object.entries(result.stringData)) {
    columns[h] = values;
  }

  return columns;
}

// ─── Main reader ───────────────────────────────────────────────────────────

export async function _readJSONOptimized(filePath: string, options: JSONReadOptions = {}): Promise<DataFrame> {
  const fd = fs.openSync(filePath, 'r');
  const fileSize = fs.fstatSync(fd).size;
  const numWorkers = options.workers ?? os.cpus().length;
  const batchRecords = options.batchSize ?? DEFAULT_BATCH_RECORDS;
  const workerPath = path.join(__dirname, '..', 'workers', 'json.worker.js');

  const isStreaming = typeof options.onBatch === 'function';

  try {
    // ── Fase 1: primer chunk para detectar schema ──────────────────────────

    const firstBuf = Buffer.allocUnsafe(Math.min(CHUNK_SIZE, fileSize));
    fs.readSync(fd, firstBuf, 0, firstBuf.length, 0);

    const { records: firstRecords } = extractRecords(firstBuf);
    const sample = firstRecords.slice(0, Math.min(500, firstRecords.length));

    if (sample.length === 0) {
      return new DataFrame({
        columns: {},
        rowCount: 0,
        headers: [],
        originalBuffer: null,
        offsets: null,
        colMap: null,
      });
    }

    const { headers, numericCols } = detectSchema(sample);
    const numericHeaders = headers.filter((h) => numericCols.has(h));
    const stringHeaders = headers.filter((h) => !numericCols.has(h));
    const colMap = Object.fromEntries(headers.map((h, i) => [h, i]));

    // ── Fase 2: worker pool persistente ────────────────────────────────────

    const initData: JSONWorkerInit = {
      headers,
      numericHeaders,
      stringHeaders,
    };

    const pool = new WorkerPool(Array.from({ length: numWorkers }, () => new Worker(workerPath, { workerData: initData })));

    const accum = isStreaming ? null : new ColumnAccumulator(numericHeaders, stringHeaders);

    const inFlight = new Set<Promise<void>>();

    async function handleResult(result: BatchResult, count: number): Promise<void> {
      if (isStreaming) {
        const columns = buildBatchColumns(result);

        const df = new DataFrame({
          columns,
          rowCount: count,
          headers,
          originalBuffer: null,
          offsets: null,
          colMap,
          metadata: {
            mode: 'stream',
            source: filePath,
          },
        });

        await options.onBatch!(df);
        return;
      }

      accum!.merge(result, count);
    }

    async function dispatch(batch: string[]): Promise<void> {
      const count = batch.length;

      const p: Promise<void> = pool.dispatch(batch).then(async (result) => {
        await handleResult(result, count);
        inFlight.delete(p);
      });

      inFlight.add(p);

      if (pool.idleCount === 0) {
        await Promise.race(inFlight);
      }
    }

    // ── Fase 3: streaming chunks ───────────────────────────────────────────

    let carry = Buffer.alloc(0);
    let bytesRead = 0;
    let pendingBatch: string[] = [];
    const chunkBuf = Buffer.allocUnsafe(CHUNK_SIZE);

    while (bytesRead < fileSize) {
      const toRead = Math.min(CHUNK_SIZE, fileSize - bytesRead);

      fs.readSync(fd, chunkBuf, 0, toRead, bytesRead);
      bytesRead += toRead;

      const combined = carry.length > 0 ? Buffer.concat([carry, chunkBuf.subarray(0, toRead)]) : Buffer.from(chunkBuf.subarray(0, toRead));

      const { records, consumedTo } = extractRecords(combined);
      carry = Buffer.from(combined.subarray(consumedTo));

      for (const r of records) {
        pendingBatch.push(r);

        if (pendingBatch.length >= batchRecords) {
          await dispatch(pendingBatch);
          pendingBatch = [];
        }
      }

      process.stdout.write(
        `\r   Progreso: ${((bytesRead / fileSize) * 100).toFixed(1)}%  ` + `${(bytesRead / 1e9).toFixed(2)} / ${(fileSize / 1e9).toFixed(2)} GB`,
      );
    }

    if (pendingBatch.length > 0) {
      await dispatch(pendingBatch);
    }

    await Promise.all(inFlight);
    await pool.terminate();

    // ── Fase 4: resultado ──────────────────────────────────────────────────

    if (isStreaming) {
      return new DataFrame({
        columns: {},
        rowCount: 0,
        headers,
        originalBuffer: null,
        offsets: null,
        colMap,
        metadata: {
          mode: 'stream',
          source: filePath,
        },
      });
    }

    const columns = accum!.build();

    return new DataFrame({
      columns,
      rowCount: accum!.totalRows,
      headers,
      originalBuffer: null,
      offsets: null,
      colMap,
    });
  } finally {
    fs.closeSync(fd);
  }
}
