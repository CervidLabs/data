/**
 * json_ingest.worker.ts — Worker persistente para ingesta JSON.
 *
 * Ciclo de vida:
 *   1. Se inicializa una vez con el schema (workerData)
 *   2. Recibe batches de strings JSON via postMessage (N veces)
 *   3. Por cada batch: parsea → flatten → separa numéricos/strings → responde
 *   4. El main thread lo termina con worker.terminate() al final
 */

import { parentPort, workerData } from 'worker_threads';
import type { JSONWorkerInit, BatchResult } from '../io/readJSON.js';

const { headers, numericHeaders, stringHeaders } = workerData as JSONWorkerInit;

const isNumeric = new Set<string>(numericHeaders);

// ─── Flatten iterativo (sin recursión) ─────────────────────────────────────

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

// ─── Handler de cada batch ─────────────────────────────────────────────────

parentPort?.on('message', (batch: string[]) => {
  const numericData: Record<string, number[]> = {};
  const stringData: Record<string, (string | null)[]> = {};

  // Pre-allocar arrays del tamaño exacto del batch — evita re-dimensionamientos
  for (const h of numericHeaders) {
    numericData[h] = new Array<number>(batch.length);
  }
  for (const h of stringHeaders) {
    stringData[h] = new Array<string | null>(batch.length);
  }

  for (let i = 0; i < batch.length; i++) {
    const flat = flattenIterative(JSON.parse(batch[i]) as Record<string, unknown>);

    for (const h of headers) {
      const v = flat[h];

      if (isNumeric.has(h)) {
        numericData[h][i] = v === null || v === undefined ? NaN : (v as number);
      } else {
        stringData[h][i] = v === null || v === undefined ? null : JSON.stringify(v);
      }
    }
  }

  const result: BatchResult = { numericData, stringData };
  parentPort?.postMessage(result);
});
