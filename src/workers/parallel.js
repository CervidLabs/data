import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class ParallelExecutor {
  constructor(filePath, options = {}) {
    this.filePath = filePath;
    this.numWorkers = options.numWorkers || Math.max(1, Math.min(os.cpus().length - 2, 8));
    this.fileSize = fs.statSync(filePath).size;
    this.headers = options.headers || [];
    this.delimiter = options.delimiter || ',';
  }

  getBufferChunks(bufferView) {
    const chunks = [];
    const size = bufferView.length;
    const targetSize = Math.floor(size / this.numWorkers);
    let start = 0;

    for (let i = 0; i < this.numWorkers; i++) {
      let end = Math.min(start + targetSize, size);
      while (end < size && bufferView[end] !== 10) { end++; }
      chunks.push({ start, end });
      start = end + 1;
      if (start >= size) break;
    }
    return chunks;
  }

  async executeIngest(meta) {
    this.headers = meta.headers;
    const sharedBuffer = meta.sharedBuffer;
    const view = new Uint8Array(sharedBuffer);
    
    // Conteo de filas para pre-asignar
    const rowCount = this.estimateRows(view) - 1;
    const chunks = this.getBufferChunks(view);
    
    console.log(`🚀 Octopus Nitro: Iniciando carga paralela...`);
    const startLoad = Date.now();

    // Pre-asignación de SharedArrayBuffers (8 bytes por Float64)
    const colBuffers = this.headers.map(() => new SharedArrayBuffer(rowCount * 8));
    const workerPath = path.resolve(__dirname, 'ingest.worker.js');

    let currentStartRow = 0;
    const workers = [];

    // Lanzamiento manual para asegurar que no haya undefined
    for (const chunk of chunks) {
      let rowsInChunk = 0;
      for (let j = chunk.start; j <= chunk.end; j++) {
        if (view[j] === 10) rowsInChunk++;
      }

      const worker = new Worker(workerPath, {
        workerData: {
          sharedBuffer,
          colBuffers,
          start: chunk.start,
          end: chunk.end,
          startRow: currentStartRow
        }
      });
      
      workers.push(worker);
      currentStartRow += rowsInChunk;
    }

    // Espera síncrona de los hilos
    await Promise.all(workers.map((w, i) => {
      return new Promise((resolve, reject) => {
        w.on('message', (msg) => { if (msg.type === 'done') resolve(); });
        w.on('error', (err) => reject(`Worker ${i} error: ${err}`));
        w.on('exit', (code) => { if (code !== 0) reject(`Worker ${i} murió`); });
      });
    }));

    workers.forEach(w => w.terminate());

    console.log(`✅ Ingesta completa en ${((Date.now() - startLoad) / 1000).toFixed(2)}s`);

    const columns = {};
    this.headers.forEach((h, i) => {
      columns[h] = new Float64Array(colBuffers[i]);
    });

    return { columns, rowCount };
  }

  estimateRows(view) {
    let count = 0;
    for (let i = 0; i < view.length; i++) {
      if (view[i] === 10) count++;
    }
    return count;
  }
}