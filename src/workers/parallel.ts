import { Worker } from 'worker_threads';
import path from 'path';
import { fileURLToPath } from 'url';
import type { ColumnData } from './../core/DataFrame.js';
type WorkerMessage = { type: 'done' } | { type: 'progress'; value: number } | { type: 'error'; error: string };
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// 1. Interfaces de Configuración y Datos
export interface TransformDefinition {
  name: string;
  inputs?: string[];
  formula: string;
}

export interface CompiledTransform {
  name: string;
  targetIdx: number;
  inputIndices: number[];
  formulaStr: string;
  argNames: string[];
}

export interface ParallelOptions {
  headers?: string[];
  numWorkers?: number;
  transforms?: TransformDefinition[];
}

interface Chunk {
  start: number;
  end: number;
}

interface IngestMeta {
  headers: string[];
  sharedBuffer: SharedArrayBuffer;
}

/**
 * ParallelExecutor - Orquestador de hilos para ingesta y transformación masiva.
 */
export class ParallelExecutor {
  private filePath: string;
  private headers: string[];
  private numWorkers: number;
  private transforms: TransformDefinition[];

  constructor(filePath: string, options: ParallelOptions = {}) {
    this.filePath = filePath;
    this.headers = options.headers ?? [];
    this.numWorkers = options.numWorkers ?? 4;
    this.transforms = options.transforms ?? [];
  }

  /**
   * Estima las filas contando saltos de línea (\n).
   */
  private estimateRows(view: Uint8Array): number {
    let count = 0;
    for (let i = 0; i < view.length; i++) {
      if (view[i] === 10) {
        count++;
      }
    }
    return count;
  }

  /**
   * Divide el buffer en trozos equitativos alineados a saltos de línea.
   */
  private getBufferChunks(view: Uint8Array): Chunk[] {
    const size = view.length;
    const chunkSize = Math.floor(size / this.numWorkers);
    const chunks: Chunk[] = [];

    for (let i = 0; i < this.numWorkers; i++) {
      let start = i * chunkSize;
      let end = i === this.numWorkers - 1 ? size : (i + 1) * chunkSize;

      // Sincronización Táctica: Ajustar al inicio de una línea real
      if (i > 0) {
        while (start < size && view[start - 1] !== 10) {
          start++;
        }
      }
      // Ajustar al final de una línea real
      while (end < size && view[end - 1] !== 10) {
        end++;
      }

      if (start < end) {
        chunks.push({ start, end });
      }
    }
    return chunks;
  }

  /**
   * Ejecuta la ingesta paralela con transformaciones in-flight.
   */
  public async executeIngest(meta: IngestMeta): Promise<{ columns: Record<string, ColumnData>; rowCount: number }> {
    this.headers = meta.headers;
    const sharedBuffer = meta.sharedBuffer;
    const view = new Uint8Array(sharedBuffer);

    const rowCount = this.estimateRows(view);
    const chunks = this.getBufferChunks(view);

    // Reserva de memoria compartida (Float64 = 8 bytes)
    const colBuffers = this.headers.map(() => new SharedArrayBuffer(rowCount * 8));

    // Compilación de Transformaciones (Pre-mapeo de índices para evitar overhead de strings en workers)
    const compiledTransforms: CompiledTransform[] = this.transforms.map((t) => {
      const inputKeys = t.inputs ?? [];
      return {
        name: t.name,
        targetIdx: this.headers.indexOf(t.name),
        inputIndices: inputKeys.map((inputName) => {
          const idx = this.headers.indexOf(inputName);
          if (idx === -1) {
            console.warn(` Advertencia: Columna de entrada "${inputName}" no encontrada para ${t.name}`);
          }
          return idx;
        }),
        formulaStr: t.formula,
        argNames: inputKeys,
      };
    });

    const workerPath = path.join(__dirname, 'ingest.worker.js');
    let currentStartRow = 0;
    const workers: Worker[] = [];

    // Lanzamiento y asignación de carga
    for (const chunk of chunks) {
      let rowsInChunk = 0;
      for (let j = chunk.start; j < chunk.end; j++) {
        if (view[j] === 10) {
          rowsInChunk++;
        }
      }

      const worker = new Worker(workerPath, {
        workerData: {
          sharedBuffer,
          colBuffers,
          start: chunk.start,
          end: chunk.end,
          startRow: currentStartRow,
          transforms: compiledTransforms,
          headers: this.headers,
        },
      });

      workers.push(worker);
      currentStartRow += rowsInChunk;
    }

    // Espera coordinada de hilos
    await Promise.all(
      workers.map(async (worker, i) => {
        return new Promise<void>((resolve, reject) => {
          worker.on('message', (msg: WorkerMessage) => {
            if (msg.type === 'done') {
              resolve();
            }
          });
          worker.on('error', (err) => reject(new Error(`Worker ${i} falló: ${err}`)));
          worker.on('exit', (code) => {
            if (code !== 0) {
              reject(new Error(`Worker ${i} terminó con código ${code}`));
            }
          });
        });
      }),
    );

    // Limpieza de recursos
    workers.forEach(async (w) => w.terminate());

    // Reconstrucción del mapa de columnas
    const columns: Record<string, ColumnData> = {};
    this.headers.forEach((header, index) => {
      columns[header] = new Float64Array(colBuffers[index]);
    });

    return { columns, rowCount };
  }
}
