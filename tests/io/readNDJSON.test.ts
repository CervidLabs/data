// tests/io/readNDJSON.test.ts
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { scanNDJSON, readNDJSONNitro } from '../../src/io/readNDJSON';

type WorkerHandler = (arg: unknown) => void;

const readString = (view: Uint8Array, start: number, end: number): string => {
    return Buffer.from(view.subarray(start, end)).toString('utf8');
};

const flatten = (
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
            flatten(value as Record<string, unknown>, nextKey, out);
        } else {
            out[nextKey] = value;
        }
    }

    return out;
};
const mockFlags = vi.hoisted(() => ({
    forceExitCode: null as number | null,
    dropBooleanBuffer: false,
    dropNumberBuffer: false,
    dropStringIdBuffer: false,
    omitDictionaryFor: null as string | null,
}));

vi.mock('worker_threads', () => {
    return {
        Worker: class {
            private handlers: Record<string, WorkerHandler[]> = {};
            private workerData: any;

            constructor(_workerPath: string, opts: { workerData?: any } = {}) {
                this.workerData = opts.workerData ?? {};

                setTimeout(() => {
                    this.run();
                }, 5);
            }

            on(event: string, cb: WorkerHandler) {
                this.handlers[event] ??= [];
                this.handlers[event].push(cb);
                return this;
            }

            private emit(event: string, payload: unknown) {
                this.handlers[event]?.forEach((cb) => cb(payload));
            }

            private run() {
                if (mockFlags.forceExitCode !== null) {
                    this.emit('exit', mockFlags.forceExitCode);
                    return;
                }
                const {
                    sharedBuffer,
                    rowStartsBuffer,
                    rowEndsBuffer,
                    startRow,
                    endRow,
                    fieldNames,
                    kinds,
                    numberBuffers,
                    booleanBuffers,
                    stringIdBuffers,
                } = this.workerData;

                const view = new Uint8Array(sharedBuffer);
                const rowStarts = new Int32Array(rowStartsBuffer);
                const rowEnds = new Int32Array(rowEndsBuffer);

                const dictionaries = fieldNames.map((fieldName: string) => ({
                    fieldName,
                    values: [''],
                }));
                if (mockFlags.dropBooleanBuffer) {
                    const idx = kinds.findIndex((kind: string) => kind === 'boolean');

                    if (idx >= 0) {
                        booleanBuffers[idx] = null;
                    }
                }

                if (mockFlags.dropStringIdBuffer) {
                    const idx = kinds.findIndex((kind: string) => kind === 'string');

                    if (idx >= 0) {
                        stringIdBuffers[idx] = null;
                    }
                }
                const dictMaps = dictionaries.map(
                    (dict: { values: string[] }) => new Map<string, number>([['', 0]]),
                );

                for (let r = startRow; r < endRow; r++) {
                    const line = readString(view, rowStarts[r], rowEnds[r]);

                    let parsed: unknown;

                    try {
                        parsed = JSON.parse(line);
                    } catch {
                        continue;
                    }

                    if (
                        parsed === null ||
                        typeof parsed !== 'object' ||
                        Array.isArray(parsed)
                    ) {
                        continue;
                    }

                    const flat = flatten(parsed as Record<string, unknown>);

                    for (let fi = 0; fi < fieldNames.length; fi++) {
                        const fieldName = fieldNames[fi];
                        const kind = kinds[fi];
                        const value = flat[fieldName];

                        if (kind === 'number') {
                            const buf = numberBuffers[fi];

                            if (buf) {
                                const col = new Float64Array(buf);
                                col[r] = typeof value === 'number' ? value : Number.NaN;
                            }

                            continue;
                        }

                        if (kind === 'boolean') {
                            const buf = booleanBuffers[fi];

                            if (buf && value === true) {
                                const bits = new Uint32Array(buf);
                                bits[r >>> 5] |= 1 << (r & 31);
                            }

                            continue;
                        }

                        const buf = stringIdBuffers[fi];

                        if (buf) {
                            const ids = new Int32Array(buf);
                            const str =
                                value === null || value === undefined
                                    ? ''
                                    : typeof value === 'string'
                                        ? value
                                        : JSON.stringify(value);

                            const dict = dictionaries[fi];
                            const map = dictMaps[fi];

                            let id = map.get(str);

                            if (id === undefined) {
                                id = dict.values.length;
                                dict.values.push(str);
                                map.set(str, id);
                            }

                            ids[r] = id;
                        }
                    }
                }
                const outputDictionaries = mockFlags.omitDictionaryFor
                    ? dictionaries.filter(
                        (dict: { fieldName: string }) =>
                            dict.fieldName !== mockFlags.omitDictionaryFor,
                    )
                    : dictionaries;

                this.emit('message', {
                    type: 'done',
                    rowCount: endRow - startRow,
                    startRow,
                    endRow,
                    dictionaries: outputDictionaries,
                });

                // No emitir exit aquí.
                // En el mock puede generar ruido asíncrono después de resolver la Promise.
            }
        },
    };
});

describe('readNDJSON Nitro', () => {
    let testDir: string;

    beforeEach(() => {
        testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cervid-ndjson-'));
        mockFlags.dropNumberBuffer = false;
        mockFlags.forceExitCode = null;
        mockFlags.dropBooleanBuffer = false;
        mockFlags.dropStringIdBuffer = false;
        mockFlags.omitDictionaryFor = null;
    });

    afterEach(() => {
        fs.rmSync(testDir, {
            recursive: true,
            force: true,
        });
    });

    const writeFile = (name: string, content: string): string => {
        const filePath = path.join(testDir, name);
        fs.writeFileSync(filePath, content);
        return filePath;
    };

    it('should scan schema and rowCount without launching eager API directly', () => {
        const filePath = writeFile(
            'events.ndjson',
            [
                JSON.stringify({
                    id: 1,
                    active: true,
                    name: 'Alpha',
                    meta: { score: 99.5 },
                    tags: ['a', 'b'],
                }),
                JSON.stringify({
                    id: 2,
                    active: false,
                    name: 'Beta',
                    meta: { score: 88.25 },
                    tags: ['x'],
                }),
            ].join('\n'),
        );

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(lazy.rowCount).toBe(2);
        expect(lazy.schema.fields).toEqual([
            { name: 'active', kind: 'boolean' },
            { name: 'id', kind: 'number' },
            { name: 'meta.score', kind: 'number' },
            { name: 'name', kind: 'string' },
            { name: 'tags', kind: 'string' },
        ]);
    });

    it('should collect all columns eagerly', async () => {
        const filePath = writeFile(
            'full.ndjson',
            [
                JSON.stringify({
                    id: 1,
                    active: true,
                    name: 'Alpha',
                    meta: { score: 10 },
                    tags: ['a', 'b'],
                }),
                JSON.stringify({
                    id: 2,
                    active: false,
                    name: 'Beta',
                    meta: { score: 20 },
                    tags: ['x'],
                }),
            ].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 2,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(2);
        expect(df.headers).toEqual([
            'active',
            'id',
            'meta.score',
            'name',
            'tags',
        ]);

        expect(df.columns.id[0]).toBe(1);
        expect(df.columns.id[1]).toBe(2);

        expect(df.columns['meta.score'][0]).toBe(10);
        expect(df.columns['meta.score'][1]).toBe(20);

        expect(df.columns.active[0]).toBe(1);
        expect(df.columns.active[1]).toBe(0);

        expect(df.columns.name[0]).toBe('Alpha');
        expect(df.columns.name[1]).toBe('Beta');

        expect(df.columns.tags[0]).toBe('["a","b"]');
        expect(df.columns.tags[1]).toBe('["x"]');
    });

    it('should select only requested columns', async () => {
        const filePath = writeFile(
            'select.ndjson',
            [
                JSON.stringify({ id: 1, name: 'A', ignored: 100 }),
                JSON.stringify({ id: 2, name: 'B', ignored: 200 }),
            ].join('\n'),
        );

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        const df = await lazy.select(['name']);

        expect(df.rowCount).toBe(2);
        expect(df.headers).toEqual(['name']);
        expect(df.columns.name[0]).toBe('A');
        expect(df.columns.name[1]).toBe('B');
        expect(df.columns.id).toBeUndefined();
        expect(df.columns.ignored).toBeUndefined();
    });

    it('should handle empty files', async () => {
        const filePath = writeFile('empty.ndjson', '');

        const lazy = scanNDJSON(filePath, {
            workers: 1,
        });

        expect(lazy.rowCount).toBe(0);
        expect(lazy.schema.fields).toEqual([]);

        const df = await lazy.collect();

        expect(df.rowCount).toBe(0);
        expect(df.headers).toEqual([]);
        expect(df.columns).toEqual({});
    });

    it('should ignore empty lines', async () => {
        const filePath = writeFile(
            'empty-lines.ndjson',
            [
                '',
                JSON.stringify({ id: 1 }),
                '',
                JSON.stringify({ id: 2 }),
                '',
            ].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(2);
        expect(df.headers).toEqual(['id']);
        expect(df.columns.id[0]).toBe(1);
        expect(df.columns.id[1]).toBe(2);
    });

    it('should handle escaped strings during schema inference', async () => {
        const filePath = writeFile(
            'escaped.ndjson',
            [
                JSON.stringify({
                    text: 'hello "quoted" world',
                    nested: {
                        value: 'a\\b',
                    },
                }),
            ].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(1);
        expect(df.headers).toContain('text');
        expect(df.headers).toContain('nested.value');
        expect(df.columns.text[0]).toBe('hello "quoted" world');
        expect(df.columns['nested.value'][0]).toBe('a\\b');
    });

    it('should infer null as string and preserve it as empty string', async () => {
        const filePath = writeFile(
            'nulls.ndjson',
            [
                JSON.stringify({ value: null }),
                JSON.stringify({ value: 'hello' }),
            ].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(2);
        expect(df.headers).toEqual(['value']);
        expect(df.columns.value[0]).toBe('');
        expect(df.columns.value[1]).toBe('hello');
    });

    it('should discover fields appearing in head, middle, and tail samples', () => {
        const rows: string[] = [];

        for (let i = 0; i < 30; i++) {
            if (i === 0) {
                rows.push(JSON.stringify({ head: 1 }));
            } else if (i === 15) {
                rows.push(JSON.stringify({ middle: true }));
            } else if (i === 29) {
                rows.push(JSON.stringify({ tail: 'done' }));
            } else {
                rows.push(JSON.stringify({ filler: i }));
            }
        }

        const filePath = writeFile('sampling.ndjson', rows.join('\n'));

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 9,
        });

        const names = lazy.schema.fields.map((f) => f.name);

        expect(names).toContain('head');
        expect(names).toContain('middle');
        expect(names).toContain('tail');
    });

    it('should return an empty DataFrame when selecting no columns', async () => {
        const filePath = writeFile(
            'no-columns.ndjson',
            [JSON.stringify({ id: 1 }), JSON.stringify({ id: 2 })].join('\n'),
        );

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        const df = await lazy.select([]);

        expect(df.rowCount).toBe(2);
        expect(df.headers).toEqual([]);
        expect(df.columns).toEqual({});
    });

    it('should handle empty objects correctly', async () => {
        const filePath = writeFile(
            'empty-obj.ndjson',
            [JSON.stringify({}), JSON.stringify({ a: 1 })].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(2);
        expect(df.headers).toEqual(['a']);
        expect(Number.isNaN(df.columns.a[0] as number)).toBe(true);
        expect(df.columns.a[1]).toBe(1);
    });

    it('should treat arrays as strings', async () => {
        const filePath = writeFile(
            'arrays.ndjson',
            JSON.stringify({ a: [1, 2, { b: 3 }] }),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(1);
        expect(df.headers).toEqual(['a']);
        expect(df.columns.a[0]).toBe('[1,2,{"b":3}]');
    });

    it('should ignore non-existent columns in select', async () => {
        const filePath = writeFile('select-missing.ndjson', JSON.stringify({ a: 1 }));

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        const df = await lazy.select(['nonexistent']);

        expect(df.rowCount).toBe(1);
        expect(df.headers).toEqual([]);
        expect(df.columns).toEqual({});
    });

    it('should distribute rows across uneven workers', async () => {
        const rows = Array.from({ length: 5 }, (_, i) => JSON.stringify({ a: i }));

        const filePath = writeFile('uneven.ndjson', rows.join('\n'));

        const df = await readNDJSONNitro(filePath, {
            workers: 3,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(5);
        expect(df.headers).toEqual(['a']);
        expect(df.columns.a[0]).toBe(0);
        expect(df.columns.a[4]).toBe(4);
    });

    it('should correctly pack boolean bits', async () => {
        const filePath = writeFile(
            'bool.ndjson',
            [
                JSON.stringify({ a: true }),
                JSON.stringify({ a: false }),
                JSON.stringify({ a: true }),
            ].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(3);
        expect(df.headers).toEqual(['a']);
        expect(df.columns.a[0]).toBe(1);
        expect(df.columns.a[1]).toBe(0);
        expect(df.columns.a[2]).toBe(1);
    });

    it('should handle malformed NDJSON lines by keeping rowCount but leaving default numeric values', async () => {
        const filePath = writeFile(
            'malformed-lines.ndjson',
            [
                JSON.stringify({ id: 1 }),
                '{ invalid json',
                JSON.stringify({ id: 3 }),
            ].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(3);
        expect(df.headers).toEqual(['id']);
        expect(df.columns.id[0]).toBe(1);
        expect(df.columns.id[1]).toBe(0);
        expect(df.columns.id[2]).toBe(3);
    });

    it('should handle primitive NDJSON rows as ignored rows with default numeric values', async () => {
        const filePath = writeFile(
            'primitive-lines.ndjson',
            [
                JSON.stringify({ id: 1 }),
                JSON.stringify(123),
                JSON.stringify(true),
                JSON.stringify(null),
                JSON.stringify({ id: 5 }),
            ].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(5);
        expect(df.headers).toEqual(['id']);
        expect(df.columns.id[0]).toBe(1);
        expect(df.columns.id[1]).toBe(0);
        expect(df.columns.id[2]).toBe(0);
        expect(df.columns.id[3]).toBe(0);
        expect(df.columns.id[4]).toBe(5);
    });
    it('should reject when worker exits with non-zero code', async () => {
        mockFlags.forceExitCode = 1;

        const filePath = writeFile('worker-fail.ndjson', JSON.stringify({ id: 1 }));

        await expect(
            readNDJSONNitro(filePath, {
                workers: 1,
                sampleRows: 10,
            }),
        ).rejects.toThrow('ndjson.worker exited with code 1');
    });
    it('should throw when boolean buffer is missing', async () => {
        mockFlags.dropBooleanBuffer = true;

        const filePath = writeFile('missing-bool.ndjson', JSON.stringify({ active: true }));

        await expect(
            readNDJSONNitro(filePath, {
                workers: 1,
                sampleRows: 10,
            }),
        ).rejects.toThrow('Missing boolean buffer: active');
    });
    it('should throw when string id buffer is missing', async () => {
        mockFlags.dropStringIdBuffer = true;

        const filePath = writeFile('missing-string.ndjson', JSON.stringify({ name: 'Alpha' }));

        await expect(
            readNDJSONNitro(filePath, {
                workers: 1,
                sampleRows: 10,
            }),
        ).rejects.toThrow('Missing stringId buffer: name');
    });
    it('should continue when a worker dictionary is missing for a string column', async () => {
        mockFlags.omitDictionaryFor = 'name';

        const filePath = writeFile(
            'missing-dict.ndjson',
            [JSON.stringify({ name: 'Alpha' }), JSON.stringify({ name: 'Beta' })].join('\n'),
        );

        const df = await readNDJSONNitro(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(df.rowCount).toBe(2);
        expect(df.headers).toEqual(['name']);
        expect(df.columns.name[0]).toBe('');
        expect(df.columns.name[1]).toBe('');
    });
    it('should infer booleans over numbers when boolean samples dominate', () => {
        const filePath = writeFile(
            'boolean-dominates.ndjson',
            [
                JSON.stringify({ value: true }),
                JSON.stringify({ value: false }),
                JSON.stringify({ value: true }),
                JSON.stringify({ value: 1 }),
            ].join('\n'),
        );

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 12,
        });

        expect(lazy.schema.fields).toEqual([
            { name: 'value', kind: 'boolean' },
        ]);
    });
    it('should infer strings when string samples dominate over numbers and booleans', () => {
        const filePath = writeFile(
            'string-dominates.ndjson',
            [
                JSON.stringify({ value: 'a' }),
                JSON.stringify({ value: 'b' }),
                JSON.stringify({ value: true }),
                JSON.stringify({ value: 1 }),
            ].join('\n'),
        );

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 12,
        });

        expect(lazy.schema.fields).toEqual([
            { name: 'value', kind: 'string' },
        ]);
    });
    it('should infer unknown structured values as string', () => {
        const filePath = writeFile(
            'object-array-string.ndjson',
            [
                JSON.stringify({ arr: [1, 2, 3] }),
                JSON.stringify({ obj: { nested: [1, 2] } }),
            ].join('\n'),
        );

        const lazy = scanNDJSON(filePath, {
            workers: 1,
            sampleRows: 10,
        });

        expect(lazy.schema.fields).toContainEqual({
            name: 'arr',
            kind: 'string',
        });

        expect(lazy.schema.fields).toContainEqual({
            name: 'obj.nested',
            kind: 'string',
        });
    });
});