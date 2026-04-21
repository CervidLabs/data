import fs from 'fs';
import os from 'os';
import path from 'path';
import { Worker } from 'worker_threads';
import { fileURLToPath } from 'url';
import { DataFrame, type ColumnData } from '../core/DataFrame.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── Types ─────────────────────────────────────────────────────────────────────

type NDJSONColumnKind = 'number' | 'boolean' | 'string';

interface NDJSONSchemaField {
  name: string;
  kind: NDJSONColumnKind;
}
export interface NDJSONSchema {
  fields: NDJSONSchemaField[];
}

interface NDJSONLineIndex {
  rowCount: number;
  rowStarts: Int32Array;
  rowEnds: Int32Array;
  startsBuffer: SharedArrayBuffer;
  endsBuffer: SharedArrayBuffer;
}

interface WorkerColumnBufferRefs {
  fieldNames: string[];
  kinds: NDJSONColumnKind[];
  numberBuffers: (SharedArrayBuffer | null)[];
  booleanBuffers: (SharedArrayBuffer | null)[]; // Uint32Array bitsets
  stringIdBuffers: (SharedArrayBuffer | null)[];
}

interface NDJSONWorkerData {
  sharedBuffer: SharedArrayBuffer;
  rowStartsBuffer: SharedArrayBuffer;
  rowEndsBuffer: SharedArrayBuffer;
  startRow: number;
  endRow: number;
  fieldNames: string[];
  kinds: NDJSONColumnKind[];
  numberBuffers: (SharedArrayBuffer | null)[];
  booleanBuffers: (SharedArrayBuffer | null)[];
  stringIdBuffers: (SharedArrayBuffer | null)[];
}

interface NDJSONWorkerColumnDictionary {
  fieldName: string;
  values: string[];
}

interface NDJSONWorkerMessage {
  type: 'done';
  rowCount: number;
  startRow: number;
  endRow: number;
  dictionaries: NDJSONWorkerColumnDictionary[];
}

interface InferStats {
  numberCount: number;
  booleanCount: number;
  stringCount: number;
}

export interface ReadNDJSONNitroOptions {
  workers?: number;
  sampleRows?: number;
}

// ── Lazy API ──────────────────────────────────────────────────────────────────

/**
 * Lazy scan handle returned by `scanNDJSON`.
 * Workers are only launched when you call `select()` or `collect()`.
 */
export interface LazyNDJSON {
  /** Schema discovered from the sampled rows. */
  readonly schema: NDJSONSchema;
  /** Total row count (exact — from full line index). */
  readonly rowCount: number;

  /**
   * Parse ONLY the requested columns.
   * For wide files this can be 80 %+ faster than `collect()`.
   */
  select(columns: string[]): Promise<DataFrame>;

  /** Parse all columns. Equivalent to a full eager read. */
  collect(): Promise<DataFrame>;
}

// ── File loading ──────────────────────────────────────────────────────────────

function loadFileToSharedBuffer(filePath: string): SharedArrayBuffer {
  const { size } = fs.statSync(filePath);
  const fd = fs.openSync(filePath, 'r');
  const buf = new SharedArrayBuffer(size);
  fs.readSync(fd, new Uint8Array(buf), 0, size, 0);
  fs.closeSync(fd);
  return buf;
}

// ── Line index ────────────────────────────────────────────────────────────────

function buildLineIndex(sharedBuffer: SharedArrayBuffer): NDJSONLineIndex {
  const view = new Uint8Array(sharedBuffer);
  const starts: number[] = [];
  const ends: number[] = [];
  let lineStart = 0;

  for (let i = 0; i < view.length; i++) {
    if (view[i] === 10) {
      // LF
      if (i > lineStart) {
        starts.push(lineStart);
        ends.push(i);
      }
      lineStart = i + 1;
    }
  }
  if (lineStart < view.length) {
    starts.push(lineStart);
    ends.push(view.length);
  }

  const rowCount = starts.length;
  const startsBuffer = new SharedArrayBuffer(rowCount * Int32Array.BYTES_PER_ELEMENT);
  const endsBuffer = new SharedArrayBuffer(rowCount * Int32Array.BYTES_PER_ELEMENT);
  const rowStarts = new Int32Array(startsBuffer);
  const rowEnds = new Int32Array(endsBuffer);
  rowStarts.set(starts);
  rowEnds.set(ends);

  return { rowCount, rowStarts, rowEnds, startsBuffer, endsBuffer };
}

// ── Stochastic sampling ────────────────────────────────────────────────────────
//
// Rather than always reading the first N rows — which misses sparse fields that
// appear only later in the file — we sample three bands: head, middle, tail.
// This catches schema drift and late-appearing columns.

function collectSampleIndices(rowCount: number, sampleRows: number): number[] {
  if (rowCount === 0) {
    return [];
  }
  const third = Math.floor(sampleRows / 3);
  const indices = new Set<number>();

  // Head: first third of sample budget
  const headLimit = Math.min(third, rowCount);
  for (let i = 0; i < headLimit; i++) {
    indices.add(i);
  }

  // Middle: centred on rowCount / 2
  const mid = Math.floor(rowCount / 2);
  const midHalf = Math.floor(third / 2);
  for (let i = Math.max(headLimit, mid - midHalf); i < Math.min(rowCount, mid + midHalf); i++) {
    indices.add(i);
  }

  // Tail: last third of sample budget
  for (let i = Math.max(0, rowCount - third); i < rowCount; i++) {
    indices.add(i);
  }

  return Array.from(indices);
}

// ── Byte-level skip utilities (take explicit `view`) ─────────────────────────

function skipStrV(v: Uint8Array, i: number, end: number): number {
  i++;
  while (i < end) {
    if (v[i] === 92) {
      i += 2;
      continue;
    }
    if (v[i] === 34) {
      return i + 1;
    }
    i++;
  }
  return i;
}

function skipValV(v: Uint8Array, i: number, end: number): number {
  if (i >= end) {
    return i;
  }
  const b = v[i];
  if (b === 34) {
    return skipStrV(v, i, end);
  }
  if (b === 123) {
    return skipObjV(v, i, end);
  }
  if (b === 91) {
    return skipArrV(v, i, end);
  }
  while (i < end) {
    const c = v[i];
    if (c === 44 || c === 125 || c === 93 || c === 10 || c === 13) {
      break;
    }
    i++;
  }
  return i;
}

function skipObjV(v: Uint8Array, i: number, end: number): number {
  if (i >= end || v[i] !== 123) {
    return i;
  }
  let depth = 1;
  i++;
  while (i < end && depth > 0) {
    const b = v[i];
    if (b === 34) {
      i = skipStrV(v, i, end);
      continue;
    }
    if (b === 123) {
      depth++;
    } else if (b === 125) {
      depth--;
    }
    i++;
  }
  return i;
}

function skipArrV(v: Uint8Array, i: number, end: number): number {
  if (i >= end || v[i] !== 91) {
    return i;
  }
  let depth = 1;
  i++;
  while (i < end && depth > 0) {
    const b = v[i];
    if (b === 34) {
      i = skipStrV(v, i, end);
      continue;
    }
    if (b === 91) {
      depth++;
    } else if (b === 93) {
      depth--;
    }
    i++;
  }
  return i;
}

// ── Byte-level type inference ─────────────────────────────────────────────────
//
// Check the first byte of a JSON value to infer its type.
// This is O(1) and needs zero parsing — just a table lookup.

function inferKindFromByte(b: number): NDJSONColumnKind {
  if (b === 34 || b === 110) {
    return 'string';
  } // '"' or 'n'(ull)
  if (b === 116 || b === 102) {
    return 'boolean';
  } // 't'(rue) or 'f'(alse)
  if ((b >= 48 && b <= 57) || b === 45) {
    return 'number';
  } // '0'-'9' or '-'
  return 'string'; // arrays, objects → stringify
}

// ── Schema discovery ──────────────────────────────────────────────────────────

const _decoder = new TextDecoder('utf-8');

/**
 * Recursively walk a JSON object at the byte level, accumulating type-inference
 * statistics per field path.  No JSON.parse required.
 */
function inferObjSchema(
  v: Uint8Array,
  i: number, // position AFTER the opening '{'
  end: number,
  prefix: string,
  stats: Map<string, InferStats>,
): number {
  while (i < end) {
    while (i < end && (v[i] === 32 || v[i] === 9 || v[i] === 13 || v[i] === 44)) {
      i++;
    }
    if (i >= end || v[i] === 125) {
      return i < end ? i + 1 : i;
    } // closing '}'
    if (v[i] !== 34) {
      i++;
      continue;
    }

    i++;
    const kS = i;
    while (i < end) {
      if (v[i] === 92) {
        i += 2;
        continue;
      }
      if (v[i] === 34) {
        break;
      }
      i++;
    }
    const kE = i;
    if (i < end) {
      i++;
    }

    while (i < end && (v[i] === 32 || v[i] === 9)) {
      i++;
    }
    if (i >= end || v[i] !== 58) {
      continue;
    }
    i++;
    while (i < end && (v[i] === 32 || v[i] === 9)) {
      i++;
    }
    if (i >= end) {
      break;
    }

    const key = _decoder.decode(v.subarray(kS, kE));
    const fullKey = prefix.length > 0 ? `${prefix}.${key}` : key;
    const fb = v[i];

    if (fb === 123) {
      // Nested object — recurse, accumulate under dot-notation prefix
      i = inferObjSchema(v, i + 1, end, fullKey, stats);
    } else {
      let s = stats.get(fullKey);
      if (!s) {
        s = { numberCount: 0, booleanCount: 0, stringCount: 0 };
        stats.set(fullKey, s);
      }

      const kind = fb === 91 ? 'string' : inferKindFromByte(fb); // arrays → 'string'
      if (kind === 'number') {
        s.numberCount++;
      } else if (kind === 'boolean') {
        s.booleanCount++;
      } else {
        s.stringCount++;
      }

      i = skipValV(v, i, end);
    }
  }
  return i;
}

function discoverSchema(sharedBuffer: SharedArrayBuffer, index: NDJSONLineIndex, sampleRows = 300): NDJSONSchema {
  const view = new Uint8Array(sharedBuffer);
  const stats = new Map<string, InferStats>();
  const indices = collectSampleIndices(index.rowCount, sampleRows);

  for (const lineIdx of indices) {
    const ls = index.rowStarts[lineIdx];
    const le = index.rowEnds[lineIdx];
    if (le <= ls) {
      continue;
    }

    // Find opening '{'
    let i = ls;
    while (i < le && view[i] !== 123) {
      i++;
    }
    if (i >= le) {
      continue;
    }

    try {
      inferObjSchema(view, i + 1, le, '', stats);
    } catch {
      /* skip malformed sample lines */
    }
  }

  const fields: NDJSONSchemaField[] = Array.from(stats.entries())
    .map(([name, s]) => {
      let kind: NDJSONColumnKind = 'string';
      if (s.numberCount >= s.booleanCount && s.numberCount >= s.stringCount) {
        kind = 'number';
      } else if (s.booleanCount >= s.numberCount && s.booleanCount >= s.stringCount) {
        kind = 'boolean';
      }
      return { name, kind };
    })
    .sort((a, b) => a.name.localeCompare(b.name));

  return { fields };
}

// ── Buffer allocation ─────────────────────────────────────────────────────────

function allocateBuffers(schema: NDJSONSchema, rowCount: number): WorkerColumnBufferRefs {
  const fieldNames: string[] = [];
  const kinds: NDJSONColumnKind[] = [];
  const numberBuffers: (SharedArrayBuffer | null)[] = [];
  const booleanBuffers: (SharedArrayBuffer | null)[] = [];
  const stringIdBuffers: (SharedArrayBuffer | null)[] = [];

  for (const f of schema.fields) {
    fieldNames.push(f.name);
    kinds.push(f.kind);

    if (f.kind === 'number') {
      numberBuffers.push(new SharedArrayBuffer(rowCount * Float64Array.BYTES_PER_ELEMENT));
      booleanBuffers.push(null);
      stringIdBuffers.push(null);
    } else if (f.kind === 'boolean') {
      numberBuffers.push(null);
      // Bit-packed: 1 bit per row → Math.ceil(rowCount / 32) Uint32 words
      booleanBuffers.push(new SharedArrayBuffer(Math.ceil(rowCount / 32) * Uint32Array.BYTES_PER_ELEMENT));
      stringIdBuffers.push(null);
    } else {
      numberBuffers.push(null);
      booleanBuffers.push(null);
      stringIdBuffers.push(new SharedArrayBuffer(rowCount * Int32Array.BYTES_PER_ELEMENT));
    }
  }

  return { fieldNames, kinds, numberBuffers, booleanBuffers, stringIdBuffers };
}

// ── Row distribution ──────────────────────────────────────────────────────────

function distributeRows(totalRows: number, workers: number): Array<{ startRow: number; endRow: number }> {
  const result: Array<{ startRow: number; endRow: number }> = [];
  const base = Math.floor(totalRows / workers);
  let rem = totalRows % workers;
  let cur = 0;

  for (let i = 0; i < workers; i++) {
    const size = base + (rem > 0 ? 1 : 0);
    if (rem > 0) {
      rem--;
    }
    const startRow = cur;
    const endRow = cur + size;
    if (startRow < endRow) {
      result.push({ startRow, endRow });
    }
    cur = endRow;
  }

  return result;
}

// ── Column materialization ────────────────────────────────────────────────────

function materializeColumns(
  schema: NDJSONSchema,
  rowCount: number,
  refs: WorkerColumnBufferRefs,
  msgs: NDJSONWorkerMessage[],
): Record<string, ColumnData> {
  const columns: Record<string, ColumnData> = {};

  // ── 1. Build global string dictionaries ──────────────────────────────────────
  const globalDicts = new Map<string, string[]>();
  for (const f of schema.fields) {
    if (f.kind !== 'string') {
      continue;
    }
    const gv = [''];
    const seen = new Set(['']);
    for (const msg of msgs) {
      const d = msg.dictionaries.find((x) => x.fieldName === f.name);
      if (!d) {
        continue;
      }
      for (const v of d.values) {
        if (!seen.has(v)) {
          seen.add(v);
          gv.push(v);
        }
      }
    }
    globalDicts.set(f.name, gv);
  }

  // ── 2. Materialize per-column ─────────────────────────────────────────────────
  for (let fi = 0; fi < schema.fields.length; fi++) {
    const f = schema.fields[fi];

    if (f.kind === 'number') {
      const buf = refs.numberBuffers[fi];
      if (!buf) {
        throw new Error(`Missing number buffer: ${f.name}`);
      }
      columns[f.name] = new Float64Array(buf);
      continue;
    }

    if (f.kind === 'boolean') {
      const buf = refs.booleanBuffers[fi];
      if (!buf) {
        throw new Error(`Missing boolean buffer: ${f.name}`);
      }
      // Expand bit-packed Uint32 → Uint8Array for DataFrame compatibility
      const bits = new Uint32Array(buf);
      const expanded = new Uint8Array(rowCount);
      for (let r = 0; r < rowCount; r++) {
        expanded[r] = (bits[r >>> 5] >>> (r & 31)) & 1;
      }
      columns[f.name] = expanded;
      continue;
    }

    // ── String: remap each worker's local IDs → global IDs, then expand ──
    const idBuf = refs.stringIdBuffers[fi];
    if (!idBuf) {
      throw new Error(`Missing stringId buffer: ${f.name}`);
    }
    const ids = new Int32Array(idBuf);
    const dict = globalDicts.get(f.name) ?? [''];

    const reverse = new Map<string, number>(dict.map((v, i) => [v, i]));

    for (const msg of msgs) {
      const d = msg.dictionaries.find((x) => x.fieldName === f.name);
      if (!d) {
        continue;
      }

      // Build local→global ID map for this worker's dictionary
      const l2g = new Map<number, number>([[0, 0]]);
      for (let lid = 1; lid < d.values.length; lid++) {
        l2g.set(lid, reverse.get(d.values[lid]) ?? 0);
      }

      // Remap only this worker's row slice
      for (let r = msg.startRow; r < msg.endRow; r++) {
        const gid = l2g.get(ids[r]);
        if (gid !== undefined) {
          ids[r] = gid;
        }
      }
    }

    // Expand int IDs → JS string array for the DataFrame
    columns[f.name] = Array.from(ids, (id) => dict[id] ?? '');
  }

  return columns;
}

// ── Worker dispatch ───────────────────────────────────────────────────────────

const _workerPath = path.join(__dirname, '..', 'workers', 'ndjson.worker.js');

async function _executeRead(
  sharedBuffer: SharedArrayBuffer,
  lineIndex: NDJSONLineIndex,
  schema: NDJSONSchema,
  numWorkers: number,
): Promise<DataFrame> {
  const refs = allocateBuffers(schema, lineIndex.rowCount);
  const ranges = distributeRows(lineIndex.rowCount, numWorkers);

  const results = await Promise.all(
    ranges.map(
      async (range) =>
        new Promise<NDJSONWorkerMessage>((resolve, reject) => {
          const w = new Worker(_workerPath, {
            workerData: {
              sharedBuffer,
              rowStartsBuffer: lineIndex.startsBuffer,
              rowEndsBuffer: lineIndex.endsBuffer,
              startRow: range.startRow,
              endRow: range.endRow,
              fieldNames: refs.fieldNames,
              kinds: refs.kinds,
              numberBuffers: refs.numberBuffers,
              booleanBuffers: refs.booleanBuffers,
              stringIdBuffers: refs.stringIdBuffers,
            } satisfies NDJSONWorkerData,
          });

          w.on('message', (msg: NDJSONWorkerMessage) => {
            if (msg.type === 'done') {
              resolve(msg);
            }
          });
          w.on('error', reject);
          w.on('exit', (code) => {
            if (code !== 0) {
              reject(new Error(`ndjson.worker exited with code ${code}`));
            }
          });
        }),
    ),
  );

  const columns = materializeColumns(schema, lineIndex.rowCount, refs, results);

  return new DataFrame({
    columns,
    rowCount: lineIndex.rowCount,
    headers: schema.fields.map((f) => f.name),
    originalBuffer: sharedBuffer,
    offsets: null,
    colMap: Object.fromEntries(schema.fields.map((f, i) => [f.name, i])),
  });
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Lazily scan an NDJSON file.
 *
 * Discovers the schema (via stochastic byte-level sampling) and builds the
 * line index synchronously, then returns a handle.  No workers are launched
 * until you call `.select()` or `.collect()`.
 *
 * @example
 * const lazy = await scanNDJSON('./events.ndjson');
 * console.log(lazy.schema); // inspect discovered fields
 *
 * // Parse only two columns — workers skip everything else
 * const df = await lazy.select(['user_id', 'score']);
 *
 * // Or parse everything
 * const full = await lazy.collect();
 */
export function scanNDJSON(filePath: string, options: ReadNDJSONNitroOptions = {}): LazyNDJSON {
  const numWorkers = options.workers ?? os.cpus().length;
  const sampleRows = options.sampleRows ?? 300;

  const sharedBuffer = loadFileToSharedBuffer(filePath);
  const lineIndex = buildLineIndex(sharedBuffer);
  const fullSchema = discoverSchema(sharedBuffer, lineIndex, sampleRows);

  return {
    schema: fullSchema,
    rowCount: lineIndex.rowCount,

    async select(columns: string[]): Promise<DataFrame> {
      const colSet: NDJSONSchema = {
        fields: fullSchema.fields.filter((f) => columns.includes(f.name)),
      };
      // Aquí el await es implícito al retornar la promesa de _executeRead
      return _executeRead(sharedBuffer, lineIndex, colSet, numWorkers);
    },

    async collect(): Promise<DataFrame> {
      return _executeRead(sharedBuffer, lineIndex, fullSchema, numWorkers);
    },
  };
}

/**
 * Fully eager read — equivalent to `(await scanNDJSON(filePath, opts)).collect()`.
 * Kept for backwards compatibility.
 */
export async function readNDJSONNitro(filePath: string, options: ReadNDJSONNitroOptions = {}): Promise<DataFrame> {
  const lazy = scanNDJSON(filePath, options);
  return lazy.collect();
}
