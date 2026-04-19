import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Cervid } from '../../src/core/Cervid';
import { DataFrame } from '../../src/core/DataFrame';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

// Mock SOLO de worker_threads
vi.mock('worker_threads', () => {
  return {
    Worker: class {
      workerData: {
        sharedBuffer: SharedArrayBuffer;
        start: number;
        end: number;
        headers: string[];
      };

      constructor(_workerPath: string, { workerData }: { workerData: { sharedBuffer: SharedArrayBuffer; start: number; end: number; headers: string[] } }) {
        this.workerData = workerData;
      }

      on(event: string, cb: (arg: unknown) => void) {
        if (event === 'message') {
          setTimeout(() => {
            const { sharedBuffer, start, end, headers } = this.workerData;

            const text = Buffer.from(sharedBuffer).toString('utf8');
            const lines = text.split('\n').filter((l) => l.length > 0);

            // header real
            const dataLines = lines.slice(1);

            // Simulación simple:
            // si start=0 y end cubre todo, cuenta todo
            // si hay múltiples workers, repartir por chunks del archivo de manera aproximada,
            // evitando contar el header como fila.
            let rowCount = 0;

            let currentPos = 0;
            for (let i = 0; i < lines.length; i++) {
              const line = lines[i];
              const lineStart = currentPos;
              const lineEnd = currentPos + line.length + 1;
              currentPos = lineEnd;

              // saltar header
              if (i === 0 && line.includes(headers[0])) continue;

              // solo contar si la línea toca este rango
              if (line.trim().length > 0 && lineEnd > start && lineStart < end) {
                rowCount++;
              }
            }

            cb({ type: 'done', rowCount });
          }, 5);
        }

        if (event === 'exit') {
          setTimeout(() => cb(0), 10);
        }

        return this;
      }

      terminate() {}
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
    fs.rmSync(testDir, { recursive: true, force: true });
  });

  it('should read CSV with single worker', async () => {
    const csvPath = path.join(testDir, 'test.csv');
    fs.writeFileSync(csvPath, 'id,value,name\n1,10.5,Alpha\n2,20.0,Beta\n3,30.5,Gamma');

    const df = await Cervid.read(csvPath, { workers: 1 });

    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rowCount).toBe(3);
    expect(df.headers).toEqual(['id', 'value', 'name']);
  });

  it('should read CSV with specific workers', async () => {
    const csvPath = path.join(testDir, 'test.csv');
    fs.writeFileSync(csvPath, 'id,value,name\n1,10.5,Alpha\n2,20.0,Beta\n3,30.5,Gamma');

    const df = await Cervid.read(csvPath, { workers: 2 });

    expect(df.headers).toEqual(['id', 'value', 'name']);
    expect(df.rowCount).toBeGreaterThan(0);
  });

  it('should read empty CSV file', async () => {
    const csvPath = path.join(testDir, 'empty.csv');
    fs.writeFileSync(csvPath, 'id,value,name\n');

    const df = await Cervid.read(csvPath, { workers: 1 });

    expect(df.rowCount).toBe(0);
    expect(df.headers).toEqual(['id', 'value', 'name']);
  });

  it('should read JSON array directly', async () => {
    const jsonPath = path.join(testDir, 'test.json');
    fs.writeFileSync(
      jsonPath,
      JSON.stringify([
        { id: 1, info: { status: 'active', code: 200 } },
        { id: 2, info: { status: 'inactive', code: 404 } }
      ])
    );

    const df = await Cervid.read(jsonPath, { type: 'json' });

    expect(df.rowCount).toBe(2);
    expect(df.headers).toContain('info.status');
    expect(df.headers).toContain('info.code');
  });

  it('should handle wrapped JSON root array', async () => {
    const jsonPath = path.join(testDir, 'wrapped.json');
    fs.writeFileSync(
      jsonPath,
      JSON.stringify({
        metadata: { version: 1 },
        items: [{ a: 1 }, { a: 2 }, { a: 3 }]
      })
    );

    const df = await Cervid.read(jsonPath, { type: 'json' });

    expect(df.rowCount).toBe(3);
    expect(df.headers).toContain('a');
  });

  it('should handle single JSON object', async () => {
    const jsonPath = path.join(testDir, 'single.json');
    fs.writeFileSync(jsonPath, JSON.stringify({ only: 'one', value: 42 }));

    const df = await Cervid.read(jsonPath);

    expect(df.rowCount).toBe(1);
    expect(df.headers).toContain('only');
    expect(df.headers).toContain('value');
  });

  it('should return empty DataFrame for primitive JSON array with current implementation', async () => {
    const jsonPath = path.join(testDir, 'primitive.json');
    fs.writeFileSync(jsonPath, JSON.stringify([1, 2, 3, 4, 5]));

    const df = await Cervid.read(jsonPath);

    // OJO: esto refleja la implementación actual de _readJSON
    // filtra solo objetos, así que los primitivos quedan fuera
    expect(df.rowCount).toBe(0);
  });

  it('should handle malformed JSON', async () => {
    const jsonPath = path.join(testDir, 'bad.json');
    fs.writeFileSync(jsonPath, '{ this is not valid json }');

    await expect(Cervid.read(jsonPath, { type: 'json' })).rejects.toThrow();
  });

  it('should create DataFrame with proper structure from JSON', async () => {
    const jsonPath = path.join(testDir, 'simple.json');
    fs.writeFileSync(
      jsonPath,
      JSON.stringify([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
      ])
    );

    const df = await Cervid.read(jsonPath);
    const data = df.toArray();

    expect(df.rowCount).toBe(2);
    expect(data[0]).toEqual({ name: 'Alice', age: 30 });
    expect(data[1]).toEqual({ name: 'Bob', age: 25 });
  });
it('should wrap a root object and flatten its primitive values', async () => {
  const jsonPath = path.join(testDir, 'root-object.json');

  fs.writeFileSync(
    jsonPath,
    JSON.stringify({
      id: 7,
      name: 'Root',
      meta: { score: 50 },
      active: true,
      nullable: null
    })
  );

  const df = await Cervid.read(jsonPath, { type: 'json' });

  expect(df.rowCount).toBe(1);
  expect(df.headers).toContain('id');
  expect(df.headers).toContain('name');
  expect(df.headers).toContain('meta.score');
  expect(df.headers).toContain('active');
  expect(df.headers).toContain('nullable');
});  
});
