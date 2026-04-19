import { describe, it, expect, vi } from 'vitest';
import { DataFrame } from '../../src/core/DataFrame';

// Mock de dependencias externas para no depender del sistema de archivos en tests unitarios
// Mock de FS y path para los tests de exportación
vi.mock('node:fs');
vi.mock('node:path', () => ({
  default: { extname: (p: string) => p.slice(p.lastIndexOf('.')) }
}));

describe('DataFrame (Cervid Core)', () => {
  const sampleData = [
    { id: 1, name: 'Alpha', score: 90.5 },
    { id: 2, name: 'Beta', score: 85.0 },
    { id: 3, name: 'Gamma', score: 92.2 }
  ];

  describe('Initialization & Static Methods', () => {
    it('should initialize empty by default', () => {
      const df = new DataFrame();
      expect(df.rowCount).toBe(0);
      expect(df.headers).toEqual([]);
    });

    it('should initialize from objects using fromObjects', () => {
      const df = DataFrame.fromObjects(sampleData);
      expect(df.rowCount).toBe(3);
      expect(df.headers).toEqual(['id', 'name', 'score']);
      expect(df.columns.id).toBeInstanceOf(Float64Array); // Los números se convierten a TypedArrays
      expect(df.columns.name).toBeInstanceOf(Array);      // Los strings a Arrays normales
    });

    it('should handle fromObjects with empty array', () => {
      const df = DataFrame.fromObjects([]);
      expect(df.rowCount).toBe(0);
    });

    it('should create from shared buffer (fromShared)', () => {
      const buffer = new ArrayBuffer(16);
      const def = {
        schema: [{ name: 'col1', dtype: 'f4', offset: 0, length: 4 }],
        buffer,
        rowCount: 4,
        source: 'memory',
        shape: [4, 1]
      };
      const df = DataFrame.fromShared(def);
      expect(df.columns.col1).toBeInstanceOf(Float32Array);
      expect(df.metadata.source).toBe('memory');
    });

    it('should throw error on unsupported dtype in fromShared', () => {
      const def = {
        schema: [{ name: 'err', dtype: 'unknown', offset: 0, length: 1 }],
        buffer: new ArrayBuffer(8),
        rowCount: 1, source: '', shape: []
      };
      expect(() => DataFrame.fromShared(def as any)).toThrow('Unsupported dtype');
    });
  });

  describe('Data Manipulation', () => {
    it('should assign new columns and validate length', () => {
      const df = DataFrame.fromObjects(sampleData);
      df.assign({ new_col: new Float64Array([10, 20, 30]) });
      expect(df.headers).toContain('new_col');
      
      // Test de error de integridad (Fila desalineada)
      expect(() => df.assign({ invalid: new Float64Array([1, 2]) }))
        .toThrow(/Error de integridad/);
    });

    it('should filter rows correctly', () => {
      const df = DataFrame.fromObjects(sampleData);
      const filtered = df.filter(['score'], (score) => (score as number) > 90);
      expect(filtered.rowCount).toBe(2); // Alpha y Gamma
    });

    it('should sort data', () => {
      const df = DataFrame.fromObjects(sampleData);
      const sorted = df.sort('score', false); // Descendente
      const scores = sorted.getCol('score' as any) as any; // Usando getCol internamente
      expect(scores[0]).toBe(92.2);
    });
  });

  describe('Math & Stats', () => {
    it('should calculate stats (mean, sum, min, max)', () => {
      const df = DataFrame.fromObjects(sampleData);
      const stats = df.stats('score');
      expect(stats?.sum).toBeCloseTo(267.7);
      expect(stats?.mean).toBeCloseTo(89.23, 1);
      expect(df.max('score')).toBe(92.2);
      expect(df.min('score')).toBe(85.0);
    });

    it('should return null stats for non-existent columns', () => {
      const df = new DataFrame();
      expect(df.stats('none')).toBeNull();
      expect(df.sum('none')).toBe(0);
    });
  });

  describe('Aggregations & Joins', () => {
    it('should perform groupBy and aggregations', () => {
      const data = [
        { cat: 'A', val: 10 },
        { cat: 'A', val: 20 },
        { cat: 'B', val: 100 }
      ];
      const df = DataFrame.fromObjects(data);
      const grouped = df.groupBy('cat', { val: ['sum', 'mean'] });
      
      expect(grouped.rowCount).toBe(2);
      // Buscamos la fila de la categoría A
      const rowA = grouped.toArray().find(r => r.cat === 'A');
      expect(rowA?.val_sum).toBe(30);
      expect(rowA?.val_mean).toBe(15);
    });

    it('should join two dataframes (inner)', () => {
      const df1 = DataFrame.fromObjects([{ id: 1, val: 'x' }]);
      const df2 = DataFrame.fromObjects([{ id: 1, extra: 'y' }]);
      const joined = df1.join(df2, 'id');
      expect(joined.rowCount).toBe(1);
      expect(joined.headers).toContain('extra');
    });
  });

  describe('Utilities & Export', () => {
    it('should return info and memory usage', () => {
      const df = DataFrame.fromObjects(sampleData);
      const info = df.info();
      expect(info.rowCount).toBe(3);
      expect(info.memoryUsage).toContain('MB');
    });

    it('should generate head and tail', () => {
      const df = DataFrame.fromObjects(sampleData);
      expect(df.head(1).rowCount).toBe(1);
      expect(df.tail(1).rowCount).toBe(1);
    });

it('should return an empty DataFrame if all rows have NA', () => {
  const df = DataFrame.fromObjects([
    { a: NaN, b: 1 },
    { a: 1, b: undefined }
  ]);
  const clean = df.dropNA();
  expect(clean.rowCount).toBe(0);
});
it('should keep rows if not all values are null when using how=all', () => {
  const df = new DataFrame({
    columns: {
      a: [null, 1],
      b: [null, null],
      c: [null, 3]
    },
    rowCount: 2,
    headers: ['a', 'b', 'c']
  });

  const clean = df.dropNA({ how: 'all' });

  expect(clean.rowCount).toBe(1); // solo elimina fila completamente null
});
  });
  describe('Joins & Merges', () => {
it('should perform a left join correctly', () => {
  const left = DataFrame.fromObjects([{ id: 1, name: 'A' }, { id: 2, name: 'B' }]);
  const right = DataFrame.fromObjects([{ id: 1, age: 20 }]);
  
  const joined = left.join(right, 'id', 'left');
  
  expect(joined.rowCount).toBe(2);
  // En TypedArrays, el null se convierte en 0 por defecto en tu fromObjects
  expect(joined.toArray()[1].age).toBeNaN(); 
});

    it('should perform a left join correctly', () => {
      const left = DataFrame.fromObjects([{ id: 1, name: 'A' }, { id: 2, name: 'B' }]);
      const right = DataFrame.fromObjects([{ id: 1, age: 20 }]);
      
      const joined = left.join(right, 'id', 'left');
      
      expect(joined.rowCount).toBe(2);
      expect(joined.toArray()[1].age).toBeNaN(); // ID 2 no tiene match
    });
  });

  // 2. TEST DE GROUPBY Y AGGREGATIONS (Líneas 720-740)
  describe('Aggregations', () => {
    it('should group by and calculate multiple stats', () => {
      const df = DataFrame.fromObjects([
        { cat: 'X', val: 10 },
        { cat: 'X', val: 20 },
        { cat: 'Y', val: 100 }
      ]);
      
      const aggregated = df.groupBy('cat', { val: ['sum', 'mean', 'max', 'min', 'count'] });
      
      const rowX = aggregated.toArray().find(r => r.cat === 'X');
      expect(rowX?.val_sum).toBe(30);
      expect(rowX?.val_mean).toBe(15);
      expect(rowX?.val_count).toBe(2);
    });

it('should handle groupByRange and groupByID', () => {
  const df = DataFrame.fromObjects([
    { id: 1, score: 10 },
    { id: 1.5, score: 20 }
  ]);
  const result = df.groupByID('id', 'score');
  
  // result[0] es el grupo '1' porque Math.floor(1) y Math.floor(1.5) son 1
  expect(result[0].avg).toBe(15); 
});
  });

  // 3. TEST DE BÚSQUEDA Y CASTING (Líneas 750-763)
  describe('Search & Type Casting', () => {
    it('should filter using str_contains', () => {
      const df = DataFrame.fromObjects([
        { name: 'Cervid Engine' },
        { name: 'Octopus DB' }
      ]);
      const filtered = df.str_contains('name', 'cervid');
      expect(filtered.rowCount).toBe(1);
    });

    it('should cast columns correctly', () => {
      const df = DataFrame.fromObjects([{ price: "10.5" }, { price: "20" }]);
      df.cast('price', 'float');
      expect(df.columns.price).toBeInstanceOf(Float64Array);
      expect(df.sum('price')).toBe(30.5);
      
      df.cast('price', 'string');
      expect(Array.isArray(df.columns.price)).toBe(true);
      expect(df.columns.price[0]).toBe("10.5");
    });
  });

  // 4. TEST DE EXPORTACIÓN Y ARCHIVOS (Líneas 671-679, 791-821)
  describe('Exports & Files', () => {
    it('should calculate cumulative sum (cumsum)', () => {
      const df = DataFrame.fromObjects([{ a: 1 }, { a: 2 }, { a: 3 }]);
      df.cumsum('a');
      expect(df.headers).toContain('a_cumsum');
      expect(df.columns.a_cumsum[2]).toBe(6);
    });

    it('should rename columns', () => {
      const df = DataFrame.fromObjects([{ old: 1 }]);
      const renamed = df.rename({ old: 'new' });
      expect(renamed.headers).toContain('new');
      expect(renamed.headers).not.toContain('old');
    });

    it('should throw error on invalid file extension for export', async () => {
      const df = new DataFrame();
      await expect(df.toCSV('test.txt')).rejects.toThrow('Invalid extension');
    });
  });

  // 5. MÉTODOS DE VISUALIZACIÓN (HEAD/TAIL/DESCRIBE)
  describe('Reporting', () => {
    it('should describe numeric columns', () => {
      const df = DataFrame.fromObjects([{ a: 1 }, { a: 10 }]);
      const spy = vi.spyOn(console, 'table');
      df.describe();
      expect(spy).toHaveBeenCalled();
    });

    it('should count unique values', () => {
      const df = DataFrame.fromObjects([{ a: 'x' }, { a: 'x' }, { a: 'y' }]);
      expect(df.nunique('a')).toBe(2);
      expect(df.value_counts('a')[0].count).toBe(2);
    });
  });
});
describe('DataFrame (extra coverage)', () => {
  it('should copy filePath and fileType when constructed from another DataFrame', () => {
    const base = new DataFrame({
      columns: { a: new Float64Array([1, 2]) },
      rowCount: 2,
      headers: ['a'],
      filePath: 'x.csv',
      fileType: 'csv'
    });

    const copy = new DataFrame(base);

    expect(copy.filePath).toBe('x.csv');
    expect(copy.fileType).toBe('csv');
  });

  it('should return null from getCol when column does not exist', () => {
    const df = new DataFrame();
    expect(df.getCol('missing')).toBeNull();
  });

  it('should build fromArray into shared Float64Array columns', () => {
    const df = new DataFrame().fromArray([
      { a: 1, b: 2 },
      { a: 3, b: 4 }
    ]);

    expect(df.rowCount).toBe(2);
    expect(df.columns.a).toBeInstanceOf(Float64Array);
    expect(df.columns.b).toBeInstanceOf(Float64Array);
    expect((df.columns.a as Float64Array)[1]).toBe(3);
  });

  it('should return exact rows with toArray', () => {
    const df = DataFrame.fromObjects([
      { a: 1, b: 'x' },
      { a: 2, b: 'y' }
    ]);

    expect(df.toArray()).toEqual([
      { a: 1, b: 'x' },
      { a: 2, b: 'y' }
    ]);
  });

  it('should assign into an empty DataFrame and inherit rowCount', () => {
    const df = new DataFrame();
    df.assign({ a: new Float64Array([10, 20, 30]) });

    expect(df.rowCount).toBe(3);
    expect(df.headers).toContain('a');
  });

  it('should execute with_label and store metadata indexer', () => {
    const df = DataFrame.fromObjects([{ txt: 'a' }, { txt: 'b' }]);

    const indexer = {
      fitTransform: vi.fn().mockReturnValue(new Float64Array([1, 2]))
    };

    const out = df.with_label([{ input: 'txt', indexer }]);

    expect(indexer.fitTransform).toHaveBeenCalledWith(df, 'txt');
    expect(out.headers).toContain('txt_indexed');
    expect(out.columns.txt_indexed).toBeInstanceOf(Float64Array);
    expect(out.metadata.indexers?.txt).toBe(indexer);
  });

  it('should execute with_columns with 1 input', () => {
    const df = DataFrame.fromObjects([{ a: 1 }, { a: 2 }]);
    df.with_columns([
      { name: 'b', inputs: ['a'], formula: (a: number) => a * 2 }
    ]);

    expect((df.columns.b as Float64Array)[0]).toBe(2);
    expect((df.columns.b as Float64Array)[1]).toBe(4);
  });

  it('should execute with_columns with 2 inputs', () => {
    const df = DataFrame.fromObjects([{ a: 1, b: 10 }, { a: 2, b: 20 }]);
    df.with_columns([
      { name: 'c', inputs: ['a', 'b'], formula: (a: number, b: number) => a + b }
    ]);

    expect((df.columns.c as Float64Array)[0]).toBe(11);
    expect((df.columns.c as Float64Array)[1]).toBe(22);
  });

  it('should execute with_columns with 4 inputs', () => {
    const df = DataFrame.fromObjects([
      { a: 1, b: 2, c: 3, d: 4 }
    ]);

    df.with_columns([
      {
        name: 'sum4',
        inputs: ['a', 'b', 'c', 'd'],
        formula: (a: number, b: number, c: number, d: number) => a + b + c + d
      }
    ]);

    expect((df.columns.sum4 as Float64Array)[0]).toBe(10);
  });

  it('should execute with_columns with fallback branch (>4 inputs)', () => {
    const df = DataFrame.fromObjects([
      { a: 1, b: 2, c: 3, d: 4, e: 5 }
    ]);

    df.with_columns([
      {
        name: 'sum5',
        inputs: ['a', 'b', 'c', 'd', 'e'],
        formula: (...args: number[]) => args.reduce((x, y) => x + y, 0)
      }
    ]);

    expect((df.columns.sum5 as Float64Array)[0]).toBe(15);
  });

  it('should call console.table in show()', () => {
    const df = DataFrame.fromObjects([{ a: 'abcdefghijklmnopqrstuvwxyz' }]);
    const spy = vi.spyOn(console, 'table').mockImplementation(() => {});
    df.show(1);
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });

  it('should return 0 mean for empty dataframe', () => {
    const df = new DataFrame({
      columns: { a: new Float64Array([]) },
      rowCount: 0,
      headers: ['a']
    });

    expect(df.mean('a')).toBe(0);
  });

  it('should return null for min/max on missing column', () => {
    const df = new DataFrame();
    expect(df.min('x')).toBeNull();
    expect(df.max('x')).toBeNull();
  });

  it('should support unique()', () => {
    const df = DataFrame.fromObjects([{ a: 'x' }, { a: 'x' }, { a: 'y' }]);
    expect(df.unique('a')).toEqual(['x', 'y']);
  });

  it('should fill nullish values in-place with fillna()', () => {
    const df = new DataFrame({
      columns: {
        a: [1, null, undefined, NaN]
      },
      rowCount: 4,
      headers: ['a']
    });

    df.fillna(999);

    expect(df.columns.a).toEqual([1, 999, 999, 999]);
  });

  it('should select only requested columns', () => {
    const df = DataFrame.fromObjects([{ a: 1, b: 2 }]);
    const out = df.select(['b']);

    expect(out.headers).toEqual(['b']);
    expect(out.columns.b).toBeDefined();
    expect(out.columns.a).toBeUndefined();
  });

  it('should cast to int/float branch and string branch', () => {
    const df = DataFrame.fromObjects([{ a: '10.5' }, { a: '20' }]);

    df.cast('a', 'int');
    expect(df.columns.a).toBeInstanceOf(Float64Array);
    expect((df.columns.a as Float64Array)[0]).toBe(10.5);

    df.cast('a', 'string');
    expect(Array.isArray(df.columns.a)).toBe(true);
    expect((df.columns.a as string[])[0]).toBe('10.5');
  });

  it('should create cumsum column', () => {
    const df = DataFrame.fromObjects([{ a: 1 }, { a: 2 }, { a: 3 }]);
    df.cumsum('a');

    expect(df.headers).toContain('a_cumsum');
    expect((df.columns.a_cumsum as Float64Array)[2]).toBe(6);
  });

  it('should call describe() with no numeric columns branch', () => {
    const df = new DataFrame({
      columns: { a: ['x', 'y'] },
      rowCount: 2,
      headers: ['a']
    });

    const infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});
    df.describe();
    expect(infoSpy).toHaveBeenCalledWith('No numeric columns found to describe.');
    infoSpy.mockRestore();
  });

  it('should create a Column with col()', () => {
    const df = DataFrame.fromObjects([{ a: 1 }]);
    const col = df.col('a');

    expect(col).toBeDefined();
  });

  it('should throw when col() is called with missing column', () => {
    const df = DataFrame.fromObjects([{ a: 1 }]);
    expect(() => df.col('missing')).toThrow('Column missing not found');
  });
  
});
