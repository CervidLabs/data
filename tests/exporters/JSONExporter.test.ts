import { describe, it, expect, vi, beforeEach } from 'vitest';
import fs from 'fs';
import { JSONExporter } from '../../src/exporters/json';
import { DataFrame } from '../../src/core/DataFrame';

vi.mock('fs', () => {
  const writeFile = vi.fn().mockResolvedValue(undefined);
  const createWriteStream = vi.fn();

  const mockedFs = {
    createWriteStream,
    promises: {
      writeFile,
    },
  };

  return {
    default: mockedFs,
    ...mockedFs,
  };
});

describe('JSONExporter (Cervid Nitro)', () => {
  let mockStream: {
    write: ReturnType<typeof vi.fn>;
    end: ReturnType<typeof vi.fn>;
    on: ReturnType<typeof vi.fn>;
    onFinish?: () => void;
    onError?: (err: Error) => void;
  };

  beforeEach(() => {
    vi.clearAllMocks();

    mockStream = {
      write: vi.fn().mockReturnValue(true),
      end: vi.fn().mockImplementation(function (this: typeof mockStream) {
        if (this.onFinish) this.onFinish();
      }),
      on: vi.fn().mockImplementation(function (
        this: typeof mockStream,
        event: string,
        cb: (...args: unknown[]) => void
      ) {
        if (event === 'finish') this.onFinish = cb as () => void;
        if (event === 'error') this.onError = cb as (err: Error) => void;
        return this;
      }),
    };

    vi.mocked(fs.createWriteStream).mockReturnValue(mockStream as never);
  });

  describe('Standard JSON Export', () => {
    it('should export a minified JSON by default', async () => {
      const df = DataFrame.fromObjects([{ a: 1, b: 2 }]);
      const exporter = new JSONExporter(df);

      const result = await exporter.export('test.json');

      expect(result.format).toBe('json');
      expect(fs.promises.writeFile).toHaveBeenCalledTimes(1);

      const jsonSent = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
      expect(jsonSent).toBe('[{"a":1,"b":2}]');
    });

it('should export a pretty-printed JSON', async () => {
  const df = DataFrame.fromObjects([{ a: 1 }]);
  const exporter = new JSONExporter(df, { pretty: true });

  await exporter.export('pretty.json');

  const jsonSent = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
  expect(typeof jsonSent).toBe('string');
  expect(jsonSent).toBe(JSON.stringify([{ a: 1 }], null, 2));
});
  });

  describe('NDJSON Export (Streaming)', () => {
    it('should export in NDJSON format (one JSON per line)', async () => {
      const df = DataFrame.fromObjects([
        { id: 1, v: 'A' },
        { id: 2, v: 'B' },
      ]);

      const exporter = new JSONExporter(df, { ndjson: true });
      const result = await exporter.export('test.ndjson');

      expect(result.format).toBe('ndjson');
      expect(fs.createWriteStream).toHaveBeenCalledTimes(1);
      expect(mockStream.write).toHaveBeenCalledTimes(2);
      expect(mockStream.write).toHaveBeenNthCalledWith(1, '{"id":1,"v":"A"}\n');
      expect(mockStream.write).toHaveBeenNthCalledWith(2, '{"id":2,"v":"B"}\n');
    });

    it('should handle large datasets and setImmediate in NDJSON', async () => {
      const df = new DataFrame({
        columns: { x: new Float64Array(100001) },
        rowCount: 100001,
        headers: ['x'],
      });

      const exporter = new JSONExporter(df, { ndjson: true });
      await exporter.export('large.ndjson');

      expect(mockStream.end).toHaveBeenCalled();
    });

    it('should reject NDJSON promise on stream error', async () => {
      mockStream.end.mockImplementation(function (this: typeof mockStream) {
        if (this.onError) this.onError(new Error('Stream Failure'));
      });

      const df = DataFrame.fromObjects([{ a: 1 }]);
      const exporter = new JSONExporter(df, { ndjson: true });

      await expect(exporter.export('err.ndjson')).rejects.toThrow('Stream Failure');
    });
  });
});