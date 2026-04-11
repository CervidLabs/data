import fs from 'fs';
import { Worker } from 'worker_threads';
import os from 'os';
import { DataFrame } from './DataFrame.js';

export class Octopus {
    /**
     * Lee un CSV a máxima velocidad usando workers paralelos.
     */
    static async read(filePath, options = {}) {
        const stats = fs.statSync(filePath);
        const fileSize = stats.size;
        const fd = fs.openSync(filePath, 'r');
        
        // 1. ANALIZAR CABECERA (Header)
        const headerBuffer = Buffer.alloc(10000); // 10KB para la cabecera
        fs.readSync(fd, headerBuffer, 0, 10000, 0);
        const firstLine = headerBuffer.toString().split('\n')[0];
        const headers = firstLine.trim().split(',');
        const totalCols = headers.length;

        // 2. CONFIGURACIÓN NITRO
        const numWorkers = options.workers || os.cpus().length;
        const capacity = options.indexerCapacity || 10_000_000;
        const useOffsets = options.useOffsets !== false; // Activo por defecto

        // Buffers de memoria compartida
        const sharedBuffer = new SharedArrayBuffer(fileSize);
        fs.readSync(fd, new Uint8Array(sharedBuffer), 0, fileSize, 0);
        fs.closeSync(fd);

        // Buffer para Offsets (2 Int32 por celda: start, end)
        let offsetBuffer = null;
        if (useOffsets) {
            offsetBuffer = new SharedArrayBuffer(capacity * totalCols * 2 * 4);
        }

        // Buffers para Columnas (Solo si son numéricas o se especifican)
        const colBuffers = headers.map(() => new SharedArrayBuffer(capacity * 8));

        // 3. REPARTO DE TRABAJO
        const chunkSize = Math.floor(fileSize / numWorkers);
        const promises = [];
        let currentRow = 0;

        for (let i = 0; i < numWorkers; i++) {
            const start = i * chunkSize;
            const end = i === numWorkers - 1 ? fileSize : (i + 1) * chunkSize;

            promises.push(new Promise((resolve) => {
                const worker = new Worker('./src/workers/ingest.worker.js', {
                    workerData: {
                        sharedBuffer,
                        offsetBuffer,
                        colBuffers,
                        start,
                        end,
                        startRow: Math.floor(capacity / numWorkers) * i,
                        headers
                    }
                });
                worker.on('message', (msg) => {
                    if (msg.type === 'done') {
                        currentRow += msg.rowCount;
                        resolve();
                    }
                });
            }));
        }

        await Promise.all(promises);

        // 4. CONSTRUCCIÓN DEL DATAFRAME
        const columns = {};
        headers.forEach((h, i) => {
            columns[h] = new Float64Array(colBuffers[i]);
        });

const colMap = {};
headers.forEach((h, i) => colMap[h] = i);
const result = {
    columns: columns,
    rowCount: currentRow,
    headers: headers,
    originalBuffer: new Uint8Array(sharedBuffer),
    offsets: offsetBuffer ? new Int32Array(offsetBuffer) : null,
    numCols: headers.length,
    colMap: colMap
};

// 🚨 ESTO ES LO VITAL: Retornar la instancia, no el objeto plano
return new DataFrame(result);
    }

    /**
     * Shorthands de conveniencia
     */
    static async readJSON(path, options = {}) {
        return await this.read(path, { ...options, type: 'json' });
    }

    static async readCSV(path, options = {}) {
        return await this.read(path, { ...options, type: 'csv' });
    }

    static fromArray(data) {
        return new DataFrame(data);
    }
}