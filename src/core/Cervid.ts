import fs from 'fs';
import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { DataFrame, ColumnData } from './DataFrame.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

type JSONValue = string | number | boolean | null | { [key: string]: JSONValue } | JSONValue[];
type JSONObject = { [key: string]: JSONValue };

// 1. Interfaces de Configuración
export interface CervidReadOptions {
  type?: 'csv' | 'json';
  workers?: number;
  indexerCapacity?: number;
  useOffsets?: boolean;
}

interface WorkerData {
  sharedBuffer: SharedArrayBuffer;
  offsetBuffer: SharedArrayBuffer | null;
  colBuffers: SharedArrayBuffer[];
  start: number;
  end: number;
  startRow: number;
  headers: string[];
}

interface WorkerMessage {
  type: 'done';
  rowCount: number;
}

/**
 * Clase Cervid: Orquestador universal de ingesta
 */
export class Cervid {
  /**
   * Punto de entrada universal. Detecta el formato y elige el motor.
   */
  static async read(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
    const extension = filePath.split('.').pop()?.toLowerCase();

    if (extension === 'json' || options.type === 'json') {
      return await this._readJSON(filePath, options);
    }

    // Por defecto, asumimos CSV (Motor Nitro)
    return await this._readCSV(filePath, options);
  }

  /**
   * Motor Nitro para CSV: Paralelización masiva con SharedArrayBuffers
   */
  private static async _readCSV(filePath: string, options: CervidReadOptions = {}): Promise<DataFrame> {
    const stats = fs.statSync(filePath);
    const fileSize = stats.size;
    const fd = fs.openSync(filePath, 'r');

    // Analizar cabecera (buffer inicial de 10kb)
    const headerBuffer = Buffer.alloc(10000);
    fs.readSync(fd, headerBuffer, 0, 10000, 0);
    const firstLine = headerBuffer.toString().split('\n')[0];
    const headers = firstLine.trim().split(',');
    const totalCols = headers.length;

    // Configuración Workers
    const numWorkers = options.workers ?? os.cpus().length;
    const capacity = options.indexerCapacity ?? 10_000_000;

    // Carga de archivo en memoria compartida
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
          // Nota: En producción .js, en desarrollo TS (NodeNext resuelve esto)
          const workerPath = path.join(__dirname, '..', 'workers', 'ingest.worker.js');

          const workerData: WorkerData = {
            sharedBuffer,
            offsetBuffer,
            colBuffers,
            start,
            end,
            startRow: Math.floor(capacity / numWorkers) * i,
            headers,
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
            if (code !== 0) reject(new Error(`Worker finalizó con código ${code}`));
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

  /**
   * Motor para JSON: Lógica de aplanamiento recursivo
   */
  private static async _readJSON(filePath: string, _options: CervidReadOptions = {}): Promise<DataFrame> {
    const raw = fs.readFileSync(filePath, 'utf8');
    const dataRaw = JSON.parse(raw) as JSONValue;

    let data: JSONValue[];

    // 1. Auto-detección de raíz
    if (!Array.isArray(dataRaw)) {
      if (dataRaw !== null && typeof dataRaw === 'object') {
        const obj = dataRaw as JSONObject;
        const rootKey = Object.keys(obj).find((key) => Array.isArray(obj[key]));
        // Si encontramos una llave con array, la usamos; si no, envolvemos el objeto en un array
        data = rootKey ? (obj[rootKey] as JSONValue[]) : [obj];
      } else {
        data = [dataRaw];
      }
    } else {
      data = dataRaw;
    }

    // 2. Aplanamiento recursivo (Flattening) tipado
    const flatten = (obj: JSONObject, prefix = ''): Record<string, unknown> => {
      const res: Record<string, unknown> = {};

      for (const [key, val] of Object.entries(obj)) {
        const fullKey = prefix ? `${prefix}.${key}` : key;

        if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
          // Recursión para objetos anidados
          Object.assign(res, flatten(val as JSONObject, fullKey));
        } else {
          // Valores primitivos o arrays (que tratamos como valores individuales por ahora)
          res[fullKey] = val;
        }
      }
      return res;
    };

    // Convertimos cada elemento del array en un objeto plano
    const flattenedData = data
      .filter((item): item is JSONObject => item !== null && typeof item === 'object' && !Array.isArray(item))
      .map((item) => flatten(item));

    // Construir configuración para DataFrame
    const allKeys = new Set<string>();
    flattenedData.forEach((row) => Object.keys(row).forEach((k) => allKeys.add(k)));
    const headers = Array.from(allKeys);
    const rowCount = flattenedData.length;
    const columns: Record<string, ColumnData> = {};
    headers.forEach((h) => {
      columns[h] = flattenedData.map((row) => row[h] ?? null);
    });
    const colMap = Object.fromEntries(headers.map((h, i) => [h, i]));

    return new DataFrame({
      columns,
      rowCount,
      headers,
      originalBuffer: null,
      offsets: null,
      colMap,
    });
  }
}
