import { describe, it, expect, vi } from 'vitest';
import { Series, IStringIndexer } from '../../src/core/Series';

describe('Series (Cervid Nitro Column)', () => {
  
  // 1. Mock de un Indexer para probar traducción de IDs a Strings
  const mockIndexer: IStringIndexer = {
    getStringById: (id: number) => {
      const dictionary: Record<number, string> = {
        101: 'Product_A',
        102: 'Product_B'
      };
      return dictionary[id];
    }
  };

  describe('Initialization & Basics', () => {
    it('should initialize with Float64Array', () => {
      const data = new Float64Array([1.1, 2.2, 3.3]);
      const series = new Series('prices', data, 'float64');
      
      expect(series.length).toBe(3);
      expect(series.get(0)).toBe(1.1);
      expect(series.type).toBe('float64');
    });

    it('should initialize with Int32Array', () => {
      const data = new Int32Array([10, 20]);
      const series = new Series('counts', data, 'int32');
      
      expect(series.get(1)).toBe(20);
      expect(series.type).toBe('int32');
    });

    it('should work with static fromRawBuffer method', () => {
      const data = new Float64Array([5, 10]);
      const series = Series.fromRawBuffer('raw', data, 'float64');
      
      expect(series instanceof Series).toBe(true);
      expect(series.name).toBe('raw');
    });
  });

  describe('Indexing & Translation (The StringIndexer Logic)', () => {
    it('should translate IDs to Strings when an indexer is present', () => {
      const data = new Int32Array([101, 102]);
      const series = new Series('products', data, 'int32', mockIndexer);
      
      // Debe devolver el string del diccionario, no el número 101
      expect(series.get(0)).toBe('Product_A');
      expect(series.get(1)).toBe('Product_B');
    });

    it('should return original ID if indexer returns null or undefined', () => {
      const data = new Int32Array([999]); // ID no existente en el mock
      const series = new Series('products', data, 'int32', mockIndexer);
      
      expect(series.get(0)).toBe(999);
    });

    it('should handle indexers that do not have the getStringById function', () => {
      // Caso borde: objeto que cumple la interfaz pero sin la función (defensivo)
      const brokenIndexer = {} as IStringIndexer;
      const series = new Series('test', new Int32Array([1]), 'int32', brokenIndexer);
      
      expect(series.get(0)).toBe(1);
    });
  });

  describe('Data Slicing', () => {
    it('should slice the data and maintain the indexer reference', () => {
      const data = new Int32Array([101, 102, 103, 104]);
      const series = new Series('full', data, 'int32', mockIndexer);
      
      const sliced = series.slice(1, 3); // Debería tomar IDs 102 y 103
      
      expect(sliced.length).toBe(2);
      expect(sliced.get(0)).toBe('Product_B'); // Traducido correctamente tras el slice
      expect(sliced.indexer).toBe(series.indexer); // Misma referencia de memoria
    });

    it('should slice without end parameter', () => {
      const data = new Float64Array([1, 2, 3]);
      const series = new Series('test', data, 'float64');
      const sliced = series.slice(1);
      
      expect(sliced.length).toBe(2);
      expect(sliced.get(0)).toBe(2);
    });
  });
});