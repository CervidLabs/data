import { parentPort, workerData } from 'worker_threads';

// 1. Definición de la interfaz de datos compartidos
interface IngestWorkerData {
  sharedBuffer: SharedArrayBuffer;
  offsetBuffer: SharedArrayBuffer | null;
  colBuffers: SharedArrayBuffer[];
  start: number;
  end: number;
  startRow: number;
  headers: string[];
}

const { sharedBuffer, offsetBuffer, colBuffers, start, end, startRow, headers } = workerData as IngestWorkerData;

// Vistas de memoria TypedArray
const view = new Uint8Array(sharedBuffer);
const offsetView = offsetBuffer ? new Int32Array(offsetBuffer) : null;
// Mapeamos los buffers de columnas a Float64Array (8 bytes por número)
const columns = colBuffers.map((b) => (b ? new Float64Array(b) : null));
const totalCols = headers.length;

/**
 * Parseo de flotantes de ultra-alta velocidad (Grado Militar)
 * Evita el uso de .toString() y parseFloat() de JS, operando directamente en bytes.
 */
function fastParseFloat(buffer: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let val = 0;
  let divisor = 1;
  let dotSeen = false;
  let i = start;
  let sign = 1;

  // Manejo de signo negativo (ASCII 45 = '-')
  if (buffer[i] === 45) {
    sign = -1;
    i++;
  }

  for (; i < end; i++) {
    const b = buffer[i];

    // Punto decimal (ASCII 46 = '.')
    if (b === 46) {
      dotSeen = true;
      continue;
    }

    // Dígitos 0-9 (ASCII 48-57)
    if (b >= 48 && b <= 57) {
      val = val * 10 + (b - 48);
      if (dotSeen) divisor *= 10;
    }
  }

  return (val / divisor) * sign;
}

/**
 * Función principal de procesamiento por hilos
 */
function processRows(): void {
  let pos = start;
  let rowIdx = startRow;
  let inQuotes = false;

  // Sincronización: Si no somos el primer worker, saltamos la primera línea
  // incompleta hasta encontrar el primer salto de línea (LF = 10)
  if (start !== 0) {
    while (pos < end && view[pos] !== 10) pos++;
    pos++;
  }

  while (pos < end) {
    let fieldStart = pos;
    let rowFinished = false;
    let col = 0;

    while (pos < end) {
      const byte = view[pos];

      // Manejo de comillas dobles (ASCII 34 = ")
      if (byte === 34) {
        // Comillas escapadas "" (RFC 4180)
        if (inQuotes && view[pos + 1] === 34) {
          pos++;
        } else {
          inQuotes = !inQuotes;
        }
      }

      // Procesar delimitadores solo fuera de comillas
      if (!inQuotes) {
        // ASCII 44 = ',', ASCII 10 = '\n'
        if (byte === 44 || byte === 10) {
          if (col < totalCols) {
            // 1. Escritura en columna numérica
            const targetCol = columns[col];
            if (targetCol) {
              targetCol[rowIdx] = fastParseFloat(view, fieldStart, pos);
            }

            // 2. Escritura de punteros (Offsets) para Strings
            if (offsetView) {
              const offsetPos = (rowIdx * totalCols + col) * 2;
              let s = fieldStart;
              let e = pos;

              // Recorte (trimming) de comillas en los punteros
              if (view[s] === 34) s++;
              if (view[e - 1] === 34) e--;
              // Manejo de retorno de carro Windows (\r = 13)
              if (view[e - 1] === 13) e--;

              offsetView[offsetPos] = s;
              offsetView[offsetPos + 1] = Math.max(s, e);
            }
          }

          if (byte === 10) {
            pos++;
            rowFinished = true;
            break;
          }

          col++;
          pos++;
          fieldStart = pos;
          continue;
        }
      }
      pos++;
    }

    if (!rowFinished && pos >= end && col > 0) {
      rowFinished = true;
    }

    if (rowFinished) {
      rowIdx++;
      inQuotes = false; // Reset de seguridad ante CSVs mal formados
    }
  }

  // Notificar al hilo principal que este sector ha sido procesado
  if (parentPort) {
    parentPort.postMessage({ type: 'done', rowCount: rowIdx - startRow });
  }
}

// Ejecución táctica
processRows();
