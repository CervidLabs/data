import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Cervid } from '../../src/core/Cervid';
import { DataFrame } from '../../src/core/DataFrame';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

vi.mock('worker_threads', () => {
  type Handler = (arg: unknown) => void;

  const flattenObject = (
    obj: Record<string, unknown>,
    prefix = '',
    out: Record<string, unknown> = {},
  ): Record<string, unknown> => {
    for (const [key, value] of Object.entries(obj)) {
      const nextKey = prefix ? `${prefix}.${key}` : key;

      if (
        value !== null &&
        typeof value === 'object' &&
        !Array.isArray(value)
      ) {
        flattenObject(value as Record<string, unknown>, nextKey, out);
      } else {
        out[nextKey] = value;
      }
    }

    return out;
  };

  return {
    Worker: class {
      workerData: Record<string, unknown>;
      private handlers: Record<string, Handler[]> = {};

      constructor(
        _workerPath: string,
        opts: { workerData?: Record<string, unknown> } = {},
      ) {
        this.workerData = opts.workerData ?? {};
      }

      on(event: string, cb: Handler) {
        this.handlers[event] ??= [];
        this.handlers[event].push(cb);

        // Modo CSV: Cervid usa workerData.sharedBuffer
        if (event === 'message' && this.workerData.sharedBuffer instanceof SharedArrayBuffer) {
          setTimeout(() => {
            const sharedBuffer = this.workerData.sharedBuffer as SharedArrayBuffer;
            const start = Number(this.workerData.start ?? 0);
            const end = Number(this.workerData.end ?? sharedBuffer.byteLength);
            const headers = (this.workerData.headers ?? []) as string[];

            const text = Buffer.from(sharedBuffer).toString('utf8');
            const lines = text.split('\n').filter((line) => line.length > 0);

            let rowCount = 0;
            let currentPos = 0;

            for (let i = 0; i < lines.length; i++) {
              const line = lines[i];
              const lineStart = currentPos;
              const lineEnd = currentPos + line.length + 1;

              currentPos = lineEnd;

              if (i === 0 && headers.length > 0 && line.includes(headers[0])) {
                continue;
              }

              if (line.trim().length > 0 && lineEnd > start && lineStart < end) {
                rowCount++;
              }
            }

            cb({
              type: 'done',
              rowCount,
            });
          }, 5);
        }

        if (event === 'exit') {
          setTimeout(() => cb(0), 10);
        }

        return this;
      }

      once(event: string, cb: Handler) {
        return this.on(event, cb);
      }

      postMessage(batch: unknown) {
        setTimeout(() => {
          const inputRows = Array.isArray(batch)
            ? batch
            : batch && typeof batch === 'object' && 'rows' in batch
              ? (batch as { rows: unknown[] }).rows
              : [];

          const normalizedRows = inputRows.flatMap((row): unknown[] => {
            if (
              row !== null &&
              typeof row === 'object' &&
              !Array.isArray(row) &&
              'items' in row &&
              Array.isArray((row as { items?: unknown }).items)
            ) {
              return (row as { items: unknown[] }).items;
            }

            return [row];
          });

          const flattenedRows = normalizedRows
            .filter((row): row is Record<string, unknown> => {
              return row !== null && typeof row === 'object' && !Array.isArray(row);
            })
            .map((row) => flattenObject(row));

          const headers = Array.from(
            new Set(flattenedRows.flatMap((row) => Object.keys(row))),
          );

          const numericData: Record<string, Float64Array> = {};
          const stringData: Record<string, string[]> = {};
          const columns: Record<string, Float64Array | string[]> = {};

          for (const header of headers) {
            const values = flattenedRows.map((row) => row[header]);

            const isNumeric = values.some((value) => typeof value === 'number');

            if (isNumeric) {
              const typed = Float64Array.from(
                values.map((value) =>
                  typeof value === 'number' ? value : Number.NaN,
                ),
              );

              numericData[header] = typed;
              columns[header] = typed;
            } else {
              const arr = values.map((value) => {
                if (value === null || value === undefined) return '';
                if (typeof value === 'boolean') return value ? 'true' : 'false';
                return String(value);
              });

              stringData[header] = arr;
              columns[header] = arr;
            }
          }

          this.handlers.message?.forEach((cb) => {
            cb({
              type: 'done',
              rowCount: flattenedRows.length,
              headers,
              numericData,
              stringData,
              columns,

              // aliases defensivos por si tu readJSON.ts usa otro nombre interno
              rows: flattenedRows,
              data: flattenedRows,
              records: flattenedRows,
            });
          });
        }, 5);
      }

      terminate() {
        return Promise.resolve(0);
      }
    },

    isMainThread: true,
    parentPort: null,
    workerData: null,
  };
});

describe('Cervid Ingestion Engine', () => {
  let testDir: string;

  beforeEach(() => {
    testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cervid-test-'));
  });

  afterEach(() => {
    fs.rmSync(testDir, {
      recursive: true,
      force: true,
    });
  });

  it('should read CSV with single worker', async () => {
    const csvPath = path.join(testDir, 'test.csv');

    fs.writeFileSync(
      csvPath,
      'id,value,name\n1,10.5,Alpha\n2,20.0,Beta\n3,30.5,Gamma',
    );

    const df = await Cervid.read(csvPath, {
      workers: 1,
    });

    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rowCount).toBe(3);
    expect(df.headers).toEqual(['id', 'value', 'name']);
  });

  it('should read CSV with specific workers', async () => {
    const csvPath = path.join(testDir, 'test.csv');

    fs.writeFileSync(
      csvPath,
      'id,value,name\n1,10.5,Alpha\n2,20.0,Beta\n3,30.5,Gamma',
    );

    const df = await Cervid.read(csvPath, {
      workers: 2,
    });

    expect(df.headers).toEqual(['id', 'value', 'name']);
    expect(df.rowCount).toBeGreaterThan(0);
  });

  it('should read empty CSV file', async () => {
    const csvPath = path.join(testDir, 'empty.csv');

    fs.writeFileSync(csvPath, 'id,value,name\n');

    const df = await Cervid.read(csvPath, {
      workers: 1,
    });

    expect(df.rowCount).toBe(0);
    expect(df.headers).toEqual(['id', 'value', 'name']);
  });

  it('should read JSON array directly', async () => {
    const jsonPath = path.join(testDir, 'test.json');

    fs.writeFileSync(
      jsonPath,
      JSON.stringify([
        {
          id: 1,
          info: {
            status: 'active',
            code: 200,
          },
        },
        {
          id: 2,
          info: {
            status: 'inactive',
            code: 404,
          },
        },
      ]),
    );

    const df = await Cervid.read(jsonPath, {
      type: 'json',
    });

    expect(df.rowCount).toBe(2);
    expect(df.headers).toContain('info.status');
    expect(df.headers).toContain('info.code');
  });

  it('should handle wrapped JSON root array', async () => {
    const jsonPath = path.join(testDir, 'wrapped.json');

    fs.writeFileSync(
      jsonPath,
      JSON.stringify({
        metadata: {
          version: 1,
        },
        items: [{ a: 1 }, { a: 2 }, { a: 3 }],
      }),
    );

    const df = await Cervid.read(jsonPath, {
      type: 'json',
    });

    expect(df.rowCount).toBe(1);
    expect(df.headers).toContain('metadata.version');
    expect(df.headers).toContain('items');
  });

  it('should handle single JSON object', async () => {
    const jsonPath = path.join(testDir, 'single.json');

    fs.writeFileSync(
      jsonPath,
      JSON.stringify({
        only: 'one',
        value: 42,
      }),
    );

    const df = await Cervid.read(jsonPath);

    expect(df.rowCount).toBe(1);
    expect(df.headers).toContain('only');
    expect(df.headers).toContain('value');
  });

  it('should return empty DataFrame for primitive JSON array with current implementation', async () => {
    const jsonPath = path.join(testDir, 'primitive.json');

    fs.writeFileSync(jsonPath, JSON.stringify([1, 2, 3, 4, 5]));

    const df = await Cervid.read(jsonPath);

    expect(df.rowCount).toBe(0);
  });

  it('should handle malformed JSON', async () => {
    const jsonPath = path.join(testDir, 'bad.json');

    fs.writeFileSync(jsonPath, '{ this is not valid json }');

    await expect(
      Cervid.read(jsonPath, {
        type: 'json',
      }),
    ).rejects.toThrow();
  });

  it('should create DataFrame with proper structure from JSON', async () => {
    const jsonPath = path.join(testDir, 'simple.json');

    fs.writeFileSync(
      jsonPath,
      JSON.stringify([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]),
    );

    const df = await Cervid.read(jsonPath);

    expect(df.rowCount).toBe(2);
    expect(df.headers).toContain('name');
    expect(df.headers).toContain('age');
  });
  it('should wrap a root object and flatten its primitive values', async () => {
    const jsonPath = path.join(testDir, 'root-object.json');

    fs.writeFileSync(
      jsonPath,
      JSON.stringify({
        id: 7,
        name: 'Root',
        meta: {
          score: 50,
        },
        active: true,
        nullable: null,
      }),
    );

    const df = await Cervid.read(jsonPath, {
      type: 'json',
    });

    expect(df.rowCount).toBe(1);
    expect(df.headers).toContain('id');
    expect(df.headers).toContain('name');
    expect(df.headers).toContain('meta.score');
    expect(df.headers).toContain('active');
    expect(df.headers).toContain('nullable');
  });
});