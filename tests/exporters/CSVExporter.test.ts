import { describe, it, expect, vi, beforeEach } from 'vitest';
import { CSVExporter } from '../../src/exporters/csv';
import { DataFrame } from '../../src/core/DataFrame';
import * as fs from 'fs';

// Mock de FS
vi.mock('fs');

describe('CSVExporter (Cervid Nitro)', () => {
  let mockStream: any;

  beforeEach(() => {
    // Creamos un mock del WriteStream de Node.js
    mockStream = {
      write: vi.fn().mockReturnValue(true),
      end: vi.fn().mockImplementation(function(this: any) {
        // Simulamos que el stream termina exitosamente
        if (this.onFinish) this.onFinish();
      }),
      on: vi.fn().mockImplementation(function(this: any, event, cb) {
        if (event === 'finish') this.onFinish = cb;
        if (event === 'error') this.onError = cb;
        return this;
      }),
    };
    (fs.createWriteStream as any).mockReturnValue(mockStream);
  });

  it('should export a standard DataFrame with headers', async () => {
    const df = DataFrame.fromObjects([
      { id: 1, name: 'Cervid' },
      { id: 2, name: 'Octopus' }
    ]);
    const exporter = new CSVExporter(df);
    const result = await exporter.export('test.csv');

    expect(result.rows).toBe(2);
    expect(mockStream.write).toHaveBeenCalledWith('id,name\n');
    expect(mockStream.write).toHaveBeenCalledWith('1,Cervid\n');
    expect(mockStream.end).toHaveBeenCalled();
  });

  it('should escape values containing delimiters, quotes or newlines (RFC 4180)', async () => {
    const df = DataFrame.fromObjects([
      { text: 'Hello, World', note: 'He said "Hi"' },
      { text: 'Line\nBreak', note: 'Normal' }
    ]);
    const exporter = new CSVExporter(df);
    await exporter.export('escaped.csv');

    // "Hello, World" -> debe estar entre comillas por la coma
    expect(mockStream.write).toHaveBeenCalledWith('"Hello, World","He said ""Hi"""\n');
    // "Line\nBreak" -> debe estar entre comillas por el salto de línea
    expect(mockStream.write).toHaveBeenCalledWith('"Line\nBreak",Normal\n');
  });

  it('should respect custom delimiters and disable headers', async () => {
    const df = DataFrame.fromObjects([{ a: 1, b: 2 }]);
    const exporter = new CSVExporter(df, { delimiter: ';', header: false });
    await exporter.export('tab.csv');

    expect(mockStream.write).not.toHaveBeenCalledWith('a;b\n'); // No hay header
    expect(mockStream.write).toHaveBeenCalledWith('1;2\n');
  });

  it('should handle null and undefined values as empty strings', async () => {
    // Forzamos un DF con nulos (evitando la inferencia de TypedArray para el test)
    const df = new DataFrame({
      columns: { col: [1, null, undefined] },
      rowCount: 3,
      headers: ['col']
    });
    
    const exporter = new CSVExporter(df);
    await exporter.export('nulls.csv');

    expect(mockStream.write).toHaveBeenCalledWith('1\n');
    expect(mockStream.write).toHaveBeenCalledWith('\n'); // Null -> vacío
    expect(mockStream.write).toHaveBeenCalledWith('\n'); // Undefined -> vacío
  });

  it('should trigger setImmediate for large datasets (Loop Breathing)', async () => {
    // Creamos un DF con más de 100,000 filas para entrar en el if (i % 100000 === 0)
    const df = new DataFrame({
      columns: { a: new Float64Array(100001) },
      rowCount: 100001,
      headers: ['a']
    });

    const exporter = new CSVExporter(df);
    const result = await exporter.export('huge.csv');
    
    expect(result.rows).toBe(100001);
    // Si no falló por timeout, es que el Promise con setImmediate funcionó
  });

  it('should reject the promise on stream error', async () => {
    mockStream.end.mockImplementation(function(this: any) {
      if (this.onError) this.onError(new Error('Disk Full'));
    });

    const df = DataFrame.fromObjects([{ a: 1 }]);
    const exporter = new CSVExporter(df);

    await expect(exporter.export('error.csv')).rejects.toThrow('Disk Full');
  });
});