import { parentPort, workerData } from 'worker_threads';

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

const { sharedBuffer, offsetBuffer, colBuffers, start, end, startRow, headers, delimiter } = workerData as WorkerData;

// Vistas de memoria
const view = new Uint8Array(sharedBuffer);
const offsetView = offsetBuffer ? new Int32Array(offsetBuffer) : null;
const columns = colBuffers.map((b) => new Float64Array(b));
const totalCols = headers.length;

// ASCII importantes
const LF = 10; // \n
const CR = 13; // \r
const QUOTE = 34; // "
const MINUS = 45; // -
const DOT = 46; // .
const ZERO = 48;
const NINE = 57;

// Delimitador configurable (coma, tab, etc.)
const delimiterByte = delimiter.charCodeAt(0);

/**
 * Parser numérico rápido sobre un rango específico del buffer.
 * Si el campo está vacío o no tiene dígitos válidos, devuelve NaN.
 */
function fastParseFloat(buffer: Uint8Array, fieldStart: number, fieldEnd: number): number {
  if (fieldStart >= fieldEnd) {
    return Number.NaN;
  }

  let i = fieldStart;
  let sign = 1;
  let intPart = 0;
  let fracPart = 0;
  let fracDivisor = 1;
  let dotSeen = false;
  let sawDigit = false;

  // Trim simple de espacios
  while (i < fieldEnd && buffer[i] === 32) {
    i++;
  }

  let j = fieldEnd;
  while (j > i && buffer[j - 1] === 32) {
    j--;
  }

  if (i >= j) {
    return Number.NaN;
  }

  // Remover comillas envolventes si existen
  if (buffer[i] === QUOTE && j > i + 1 && buffer[j - 1] === QUOTE) {
    i++;
    j--;
  }

  // Signo negativo
  if (buffer[i] === MINUS) {
    sign = -1;
    i++;
  }

  for (; i < j; i++) {
    const b = buffer[i];

    if (b === DOT) {
      if (dotSeen) {
        return Number.NaN;
      }
      dotSeen = true;
      continue;
    }

    if (b >= ZERO && b <= NINE) {
      sawDigit = true;
      const digit = b - ZERO;

      if (!dotSeen) {
        intPart = intPart * 10 + digit;
      } else {
        fracPart = fracPart * 10 + digit;
        fracDivisor *= 10;
      }
      continue;
    }

    // Permitir null/undefined/vacío como NaN
    return Number.NaN;
  }

  if (!sawDigit) {
    return Number.NaN;
  }

  return sign * (intPart + fracPart / fracDivisor);
}

/**
 * Guarda offsets del campo para acceso posterior a strings.
 */
function writeOffsets(rowIdx: number, col: number, fieldStart: number, fieldEnd: number): void {
  if (!offsetView) {
    return;
  }

  let s = fieldStart;
  let e = fieldEnd;

  // Trim CR de Windows
  if (e > s && view[e - 1] === CR) {
    e--;
  }

  // Trim comillas envolventes
  if (e > s && view[s] === QUOTE) {
    s++;
  }
  if (e > s && view[e - 1] === QUOTE) {
    e--;
  }

  const offsetPos = (rowIdx * totalCols + col) * 2;
  offsetView[offsetPos] = s;
  offsetView[offsetPos + 1] = Math.max(s, e);
}

/**
 * Escribe el valor numérico del campo en la columna correspondiente.
 */
function writeNumericValue(rowIdx: number, col: number, fieldStart: number, fieldEnd: number): void {
  const targetCol = columns[col];
  targetCol[rowIdx] = fastParseFloat(view, fieldStart, fieldEnd);
}

/**
 * Procesa una fila completa desde fieldStart hasta fieldEnd por columna.
 */
function finalizeField(rowIdx: number, col: number, fieldStart: number, fieldEnd: number): void {
  if (col >= totalCols) {
    return;
  }

  writeNumericValue(rowIdx, col, fieldStart, fieldEnd);
  writeOffsets(rowIdx, col, fieldStart, fieldEnd);
}

/**
 * Función principal del worker.
 */
function processRows(): void {
  let pos = start;
  let rowIdx = startRow;
  let inQuotes = false;

  // Si no somos el primer worker, saltamos hasta la siguiente línea completa
  if (start !== 0) {
    while (pos < end && view[pos] !== LF) {
      pos++;
    }
    if (pos < end) {
      pos++;
    }
  } else {
    // Worker 0: saltar cabecera completa
    while (pos < end && view[pos] !== LF) {
      pos++;
    }
    if (pos < end) {
      pos++;
    }
  }

  while (pos < end) {
    let fieldStart = pos;
    let col = 0;
    let rowFinished = false;

    while (pos < end) {
      const byte = view[pos];

      // Manejo de comillas
      if (byte === QUOTE) {
        // Comillas escapadas ""
        if (inQuotes && pos + 1 < end && view[pos + 1] === QUOTE) {
          pos += 2;
          continue;
        }
        inQuotes = !inQuotes;
        pos++;
        continue;
      }

      if (!inQuotes) {
        // Delimitador configurable
        if (byte === delimiterByte) {
          finalizeField(rowIdx, col, fieldStart, pos);
          col++;
          pos++;
          fieldStart = pos;
          continue;
        }

        // Fin de línea
        if (byte === LF) {
          finalizeField(rowIdx, col, fieldStart, pos);
          pos++;
          rowFinished = true;
          break;
        }
      }

      pos++;
    }

    // Último campo si el chunk terminó sin \n
    if (!rowFinished && pos >= end) {
      // Solo cerrar fila si realmente había contenido
      if (fieldStart < pos || col > 0) {
        finalizeField(rowIdx, col, fieldStart, pos);
        rowFinished = true;
      }
    }

    if (rowFinished) {
      rowIdx++;
      inQuotes = false;
    }
  }

  const message: WorkerMessage = {
    type: 'done',
    rowCount: rowIdx - startRow,
  };

  parentPort?.postMessage(message);
}

processRows();
