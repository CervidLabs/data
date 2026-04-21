import { parentPort, workerData } from 'worker_threads';
import { TextDecoder } from 'util';

// ── Types ─────────────────────────────────────────────────────────────────────

type NDJSONColumnKind = 'number' | 'boolean' | 'string';

interface NDJSONWorkerData {
  sharedBuffer: SharedArrayBuffer;
  rowStartsBuffer: SharedArrayBuffer;
  rowEndsBuffer: SharedArrayBuffer;
  startRow: number;
  endRow: number;
  fieldNames: string[];
  kinds: NDJSONColumnKind[];
  numberBuffers: (SharedArrayBuffer | null)[];
  booleanBuffers: (SharedArrayBuffer | null)[]; // Uint32Array bitsets — 1 bit per row
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

// ── Module-level setup ────────────────────────────────────────────────────────

const { sharedBuffer, rowStartsBuffer, rowEndsBuffer, startRow, endRow, fieldNames, kinds, numberBuffers, booleanBuffers, stringIdBuffers } =
  workerData as NDJSONWorkerData;

const fileView = new Uint8Array(sharedBuffer);
const rowStarts = new Int32Array(rowStartsBuffer);
const rowEnds = new Int32Array(rowEndsBuffer);
const decoder = new TextDecoder('utf-8');

// Typed views over the shared column buffers
const numCols = numberBuffers.map((b) => (b ? new Float64Array(b) : null));
const boolBitCols = booleanBuffers.map((b) => (b ? new Uint32Array(b) : null));
const strIdCols = stringIdBuffers.map((b) => (b ? new Int32Array(b) : null));

// O(1) field-name → column-index lookup
const fieldMap = new Map<string, number>(fieldNames.map((n, i) => [n, i]));

// ── Local string dictionaries (per string column) ─────────────────────────────

const localDicts = new Map<string, string[]>();
const localReverse = new Map<string, Map<string, number>>();

for (let i = 0; i < fieldNames.length; i++) {
  if (kinds[i] === 'string') {
    localDicts.set(fieldNames[i], ['']);
    localReverse.set(fieldNames[i], new Map([['', 0]]));
  }
}

// ── Byte-level skip utilities ─────────────────────────────────────────────────
// All functions take/return byte offsets into fileView.

/** Skip a JSON string whose opening `"` is at `i`. Returns position after closing `"`. */
function skipStr(i: number, end: number): number {
  i++; // past opening '"'
  while (i < end) {
    if (fileView[i] === 92) {
      i += 2;
      continue;
    } // backslash escape
    if (fileView[i] === 34) {
      return i + 1;
    } // closing '"'
    i++;
  }
  return i;
}

/** Skip any JSON value. Returns position after the value. */
function skipVal(i: number, end: number): number {
  if (i >= end) {
    return i;
  }
  const b = fileView[i];
  if (b === 34) {
    return skipStr(i, end);
  }
  if (b === 123) {
    return skipObj(i, end);
  }
  if (b === 91) {
    return skipArr(i, end);
  }
  // number / boolean / null — scan until delimiter
  while (i < end) {
    const c = fileView[i];
    if (c === 44 || c === 125 || c === 93 || c === 10 || c === 13) {
      break;
    }
    i++;
  }
  return i;
}

function skipObj(i: number, end: number): number {
  if (i >= end || fileView[i] !== 123) {
    return i;
  }
  let depth = 1;
  i++;
  while (i < end && depth > 0) {
    const b = fileView[i];
    if (b === 34) {
      i = skipStr(i, end);
      continue;
    }
    if (b === 123) {
      depth++;
    } else if (b === 125) {
      depth--;
    }
    i++;
  }
  return i; // one past the matching '}'
}

function skipArr(i: number, end: number): number {
  if (i >= end || fileView[i] !== 91) {
    return i;
  }
  let depth = 1;
  i++;
  while (i < end && depth > 0) {
    const b = fileView[i];
    if (b === 34) {
      i = skipStr(i, end);
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

// ── Fast number parsing ───────────────────────────────────────────────────────

/** Find the byte offset immediately after a JSON number starting at `start`. */
function numEnd(start: number, end: number): number {
  let i = start;
  if (i < end && fileView[i] === 45) {
    i++;
  } // leading '-'
  while (i < end) {
    const b = fileView[i];
    if ((b >= 48 && b <= 57) || b === 46 || b === 101 || b === 69 || b === 43 || b === 45) {
      i++;
    } else {
      break;
    }
  }
  return i;
}

/**
 * Parse a float64 from raw bytes.
 * Pure integer fast-path: avoids TextDecoder and parseFloat entirely.
 * Floating-point values fall back to parseFloat on the minimal byte slice.
 */
function parseNum(start: number, end: number): number {
  let i = start;
  let neg = false;
  if (fileView[i] === 45) {
    neg = true;
    i++;
  } // '-'

  let v = 0;
  let isInt = true;

  while (i < end) {
    const b = fileView[i];
    if (b >= 48 && b <= 57) {
      v = v * 10 + (b - 48);
      i++;
    } else if (b === 46 || b === 101 || b === 69) {
      isInt = false;
      break;
    } else {
      break;
    }
  }

  if (isInt) {
    return neg ? -v : v;
  }

  // Float: decode only the number slice — vastly cheaper than JSON.parse on the full line
  return parseFloat(decoder.decode(fileView.subarray(start, numEnd(start, end))));
}

// ── String writing ────────────────────────────────────────────────────────────

function writeStrId(fieldName: string, value: string, colIdx: number, row: number): void {
  const col = strIdCols[colIdx];
  if (!col) {
    return;
  }
  const rev = localReverse.get(fieldName)!;
  const dict = localDicts.get(fieldName)!;
  let id = rev.get(value);
  if (id === undefined) {
    id = dict.length;
    dict.push(value);
    rev.set(value, id);
  }
  col[row] = id;
}

// ── Value writing ─────────────────────────────────────────────────────────────

/** Extract, parse, and write the value at `i` into the appropriate column buffer. */
function writeVal(i: number, end: number, colIdx: number, row: number): number {
  const kind = kinds[colIdx];
  const fb = fileView[i]; // first byte of value — drives branching

  // ── Number ──────────────────────────────────────────────────────────────────
  if (kind === 'number') {
    const ne = numEnd(i, end);
    const col = numCols[colIdx];
    if (col) {
      col[row] = parseNum(i, ne);
    }
    return ne;
  }

  // ── Boolean (bit-packed Uint32) ──────────────────────────────────────────────
  // Bit layout: word = row >>> 5, bit = row & 31
  // false is the default (SharedArrayBuffer zero-initialized); only set bits for true.
  if (kind === 'boolean') {
    const col = boolBitCols[colIdx];
    if (col && fb === 116) {
      // 't' → true
      Atomics.or(col, row >>> 5, 1 << (row & 31));
    }
    return skipVal(i, end);
  }

  // ── String ───────────────────────────────────────────────────────────────────
  const fieldName = fieldNames[colIdx];

  if (fb === 110) {
    // 'n' → null → empty string
    writeStrId(fieldName, '', colIdx, row);
    return skipVal(i, end);
  }

  if (fb === 34) {
    // '"' → actual JSON string
    let j = i + 1; // past opening '"'
    const sStart = j;
    let hasEscape = false;

    while (j < end) {
      if (fileView[j] === 92) {
        hasEscape = true;
        j += 2;
        continue;
      }
      if (fileView[j] === 34) {
        break;
      } // closing '"'
      j++;
    }
    const sEnd = j;
    if (j < end) {
      j++;
    } // past closing '"'

    const raw = decoder.decode(fileView.subarray(sStart, sEnd));
    // Only pay full JSON.parse cost when escape sequences are present (rare)
    writeStrId(fieldName, hasEscape ? (JSON.parse(`"${raw}"`) as string) : raw, colIdx, row);
    return j;
  }

  // number/bool landed in a string-typed column — stringify the raw bytes
  const ne = skipVal(i, end);
  writeStrId(fieldName, decoder.decode(fileView.subarray(i, ne)), colIdx, row);
  return ne;
}

// ── Core byte-level JSON object scanner ──────────────────────────────────────

/**
 * Scan a JSON object, starting AFTER the opening `{`.
 * Recursively descends into nested objects to build dot-notation paths.
 * Returns position after the closing `}`.
 *
 * No JSON.parse. No intermediate JavaScript objects. All values are written
 * directly into SharedArrayBuffer columns via writeVal / writeStrId.
 */
function scanObj(i: number, end: number, row: number, prefix: string): number {
  while (i < end) {
    // Skip whitespace + commas between fields
    while (i < end && (fileView[i] === 32 || fileView[i] === 9 || fileView[i] === 13 || fileView[i] === 44)) {
      i++;
    }

    if (i >= end || fileView[i] === 125) {
      return i < end ? i + 1 : i;
    } // closing '}'

    if (fileView[i] !== 34) {
      i++;
      continue;
    } // not a key string — skip stray byte

    // ── Read key ──
    i++; // past opening '"'
    const kStart = i;
    while (i < end) {
      if (fileView[i] === 92) {
        i += 2;
        continue;
      } // escaped char inside key
      if (fileView[i] === 34) {
        break;
      }
      i++;
    }
    const kEnd = i;
    if (i < end) {
      i++;
    } // past closing '"'

    // ── Find ':' ──
    while (i < end && (fileView[i] === 32 || fileView[i] === 9)) {
      i++;
    }
    if (i >= end || fileView[i] !== 58) {
      continue;
    } // malformed, skip this field
    i++; // past ':'
    while (i < end && (fileView[i] === 32 || fileView[i] === 9)) {
      i++;
    }
    if (i >= end) {
      break;
    }

    // Decode key only when it might be in the fieldMap (TextDecoder on a tiny buffer is fast)
    const key = decoder.decode(fileView.subarray(kStart, kEnd));
    const fullKey = prefix.length > 0 ? `${prefix}.${key}` : key;
    const fb = fileView[i];

    if (fb === 123) {
      // Nested object — recurse with extended dot-notation prefix
      i = scanObj(i + 1, end, row, fullKey);
    } else if (fb === 91) {
      // Array — store raw JSON string if field is needed, otherwise skip
      const colIdx = fieldMap.get(fullKey);
      if (colIdx !== undefined) {
        const aEnd = skipArr(i, end);
        writeStrId(fullKey, decoder.decode(fileView.subarray(i, aEnd)), colIdx, row);
        i = aEnd;
      } else {
        i = skipArr(i, end);
      }
    } else {
      const colIdx = fieldMap.get(fullKey);
      i = colIdx !== undefined ? writeVal(i, end, colIdx, row) : skipVal(i, end);
    }
  }
  return i;
}

// ── Main ──────────────────────────────────────────────────────────────────────

function main(): void {
  for (let row = startRow; row < endRow; row++) {
    let i = rowStarts[row];
    const end = rowEnds[row];

    // Fast-forward to the opening '{'
    while (i < end && fileView[i] !== 123) {
      i++;
    }
    if (i >= end) {
      continue;
    }

    try {
      scanObj(i + 1, end, row, '');
    } catch {
      // Malformed line — leave column slots at their zero-initialized defaults
    }
  }

  // Collect local dictionaries for the main thread to reconcile
  const dicts: NDJSONWorkerColumnDictionary[] = [];
  for (const [fieldName, values] of localDicts.entries()) {
    dicts.push({ fieldName, values });
  }

  parentPort?.postMessage({
    type: 'done',
    rowCount: endRow - startRow,
    startRow,
    endRow,
    dictionaries: dicts,
  } satisfies NDJSONWorkerMessage);
}

main();
