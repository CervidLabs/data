import { describe, it, expect, vi, beforeEach } from 'vitest';
import fs from 'fs';
import { TXTExporter } from '../../src/exporters/txt';
import { DataFrame } from '../../src/core/DataFrame';

vi.mock('fs', () => {
  const writeFile = vi.fn().mockResolvedValue(undefined);

  const mockedFs = {
    promises: {
      writeFile,
    },
  };

  return {
    default: mockedFs,
    ...mockedFs,
  };
});

describe('TXTExporter (Cervid Nitro)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should export TXT with header by default', async () => {
    const df = DataFrame.fromObjects([
      { a: 1, b: 'x' },
      { a: 2, b: 'y' },
    ]);

    const exporter = new TXTExporter(df);
    const result = await exporter.export('test.txt');

    expect(result).toEqual({
      path: 'test.txt',
      rows: 2,
      totalRows: 2,
    });

    expect(fs.promises.writeFile).toHaveBeenCalledTimes(1);

    const content = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
    expect(typeof content).toBe('string');

    const text = String(content);

    expect(text).toContain('a | b');
    expect(text).toContain('1 | x');
    expect(text).toContain('2 | y');
  });

  it('should export TXT without header when header is false', async () => {
    const df = DataFrame.fromObjects([
      { a: 1, b: 'x' },
      { a: 2, b: 'y' },
    ]);

    const exporter = new TXTExporter(df, { header: false });
    await exporter.export('no-header.txt');

    const content = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
    const text = String(content);

    expect(text).not.toContain('a | b');
    expect(text).toContain('1 | x');
    expect(text).toContain('2 | y');
  });

  it('should use a custom delimiter', async () => {
    const df = DataFrame.fromObjects([
      { a: 1, b: 'x' },
    ]);

    const exporter = new TXTExporter(df, { delimiter: ' ; ' });
    await exporter.export('custom-delimiter.txt');

    const content = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
    const text = String(content);

    expect(text).toContain('a ; b');
    expect(text).toContain('1 ; x');
  });

  it('should respect maxRows and append truncation footer', async () => {
    const df = DataFrame.fromObjects([
      { a: 1, b: 'x' },
      { a: 2, b: 'y' },
      { a: 3, b: 'z' },
    ]);

    const exporter = new TXTExporter(df, { maxRows: 2 });
    const result = await exporter.export('limited.txt');

    expect(result).toEqual({
      path: 'limited.txt',
      rows: 2,
      totalRows: 3,
    });

    const content = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
    const text = String(content);

    expect(text).toContain('1 | x');
    expect(text).toContain('2 | y');
    expect(text).not.toContain('3 | z');
    expect(text).toContain('... y 1 filas más');
  });

  it('should truncate long cell values to 50 characters', async () => {
    const longText = 'a'.repeat(80);

    const df = DataFrame.fromObjects([
      { col: longText },
    ]);

    const exporter = new TXTExporter(df);
    await exporter.export('truncate.txt');

    const content = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
    const text = String(content);

    const expectedTruncated = `${'a'.repeat(47)}...`;
    expect(text).toContain(expectedTruncated);
    expect(text).not.toContain(longText);
  });

  it('should render null and undefined as empty strings', async () => {
    const df = new DataFrame({
      columns: {
        a: [1, null, 3],
        b: ['x', undefined, 'z'],
      },
      rowCount: 3,
      headers: ['a', 'b'],
    });

    const exporter = new TXTExporter(df);
    await exporter.export('nullish.txt');

    const content = vi.mocked(fs.promises.writeFile).mock.calls[0]?.[1];
    const text = String(content);

    expect(text).toContain('1 | x');
    expect(text).toContain('3 | z');

    // Validamos que la fila con nullish no explote
    // y que represente vacíos en texto
    const lines = text.split('\n');
    expect(lines.some((line) => line.includes('  | '))).toBe(true);
  });

  it('should pass the configured encoding to writeFile', async () => {
    const df = DataFrame.fromObjects([
      { a: 1 },
    ]);

    const exporter = new TXTExporter(df, { encoding: 'utf8' });
    await exporter.export('encoding.txt');

    expect(fs.promises.writeFile).toHaveBeenCalledTimes(1);

    const args = vi.mocked(fs.promises.writeFile).mock.calls[0];
    expect(args?.[0]).toBe('encoding.txt');
    expect(args?.[2]).toBe('utf8');
  });
});