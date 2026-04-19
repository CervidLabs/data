import { describe, it, expect, beforeEach } from 'vitest';
import { DataFrame } from '../../src/core/DataFrame';
import { Column } from '../../src/core/Column';

describe('Column (Cervid Nitro)', () => {
  let df: DataFrame;
  let colA: Column;
  let colB: Column;

  beforeEach(() => {
    // Inicializamos un DataFrame base para cada test
    df = DataFrame.fromObjects([
      { a: 10, b: 2, t: '2026-01-01T12:00:00Z' },
      { a: 20, b: 5, t: '2026-01-01T15:00:00Z' }
    ]);
    // Accedemos a las columnas a través del método col() del DataFrame
    colA = df.col('a');
    colB = df.col('b');
  });

  describe('Arithmetic Operations', () => {
    it('should add scalars and other columns', () => {
      colA.add(5); // 10+5, 20+5
      expect(colA.data[0]).toBe(15);
      
      colA.add(colB); // 15+2, 25+5
      expect(colA.data[0]).toBe(17);
      expect(colA.data[1]).toBe(30);
    });

    it('should subtract scalars and other columns', () => {
      colA.sub(5); // 10-5, 20-5
      expect(colA.data[0]).toBe(5);
      
      colA.sub(colB); // 5-2, 15-5
      expect(colA.data[0]).toBe(3);
      expect(colA.data[1]).toBe(10);
    });

    it('should multiply by scalars and other columns', () => {
      colA.mul(2); // 10*2, 20*2
      expect(colA.data[0]).toBe(20);
      
      colA.mul(colB); // 20*2, 40*5
      expect(colA.data[0]).toBe(40);
      expect(colA.data[1]).toBe(200);
    });

    it('should divide by scalars and other columns safely', () => {
      colA.div(2); // 10/2, 20/2
      expect(colA.data[0]).toBe(5);
      
      colA.div(colB); // 5/2, 10/5
      expect(colA.data[0]).toBe(2.5);
      expect(colA.data[1]).toBe(2);
    });

    it('should prevent division by zero using default divisor 1', () => {
      const colZero = new Column('z', new Float64Array([0, 0]), df);
      colA.div(0); // Escalar 0 -> divisor 1
      expect(colA.data[0]).toBe(10);
      
      colA.div(colZero); // Columna 0 -> divisor 1
      expect(colA.data[1]).toBe(20);
    });
  });

  describe('DateTime Transformations', () => {
    it('should convert strings to datetime timestamps (ms)', () => {
      const colT = df.col('t');
      colT.to_datetime();
      
      expect(typeof colT.data[0]).toBe('number');
      // Verificamos que sea un timestamp válido
      expect(colT.data[0]).toBeGreaterThan(1700000000000);
    });

    it('should extract hour and create a new column in parentDf', () => {
      const colT = df.col('t');
      colT.to_datetime();
      
      // Extraemos la hora (UTC por defecto en el test, offset 0 para simplicidad)
      colT.extract_hour(0); 
      
      const hourColName = 't_hour';
      expect(df.headers).toContain(hourColName);
      expect(df.columns[hourColName]).toBeInstanceOf(Float64Array);
      
      // 12:00:00 -> 12, 15:00:00 -> 15
      expect(df.columns[hourColName][0]).toBe(12);
      expect(df.columns[hourColName][1]).toBe(15);
    });

    it('should handle negative offsets and day wrapping in extract_hour', () => {
      // Un timestamp de la 1 AM UTC
      const df2 = DataFrame.fromObjects([{ t: '2026-01-01T01:00:00Z' }]);
      const col = df2.col('t').to_datetime();
      
      // Aplicamos un offset de 2 horas (7200s) hacia atrás para forzar el "wrapping"
      col.extract_hour(7200); 
      
      // 01:00 - 2 horas = 23:00 del día anterior
      expect(df2.columns['t_hour'][0]).toBe(23);
    });
  });

  describe('Integrity & Error Handling', () => {
    it('should throw error if column name does not exist in DataFrame', () => {
      expect(() => df.col('non_existent')).toThrow(/not found/);
    });

    it('should maintain the link with parentDf after mutation', () => {
      colA.add(10);
      // El cambio en Column debe reflejarse en el DataFrame original
      expect((df.columns['a'] as any)[0]).toBe(20);
    });
  });
  describe('Column - Branch Coverage (The 100% Push)', () => {
  it('should cover all arithmetic branch combinations', () => {
    const df = DataFrame.fromObjects([{ a: 10, b: 2 }]);
    const colA = df.col('a');
    const colB = df.col('b');

    // Cubrimos ramas de Escalares
    colA.add(1).sub(1).mul(1).div(1);
    
    // Cubrimos ramas de Columnas
    colA.add(colB).sub(colB).mul(colB).div(colB);
    
    expect(colA.data[0]).toBe(10);
  });

  it('should cover the existing header branch in extract_hour', () => {
    const df = DataFrame.fromObjects([{ t: '2026-01-01T12:00:00Z' }]);
    const colT = df.col('t').to_datetime();

    // Primera ejecución: Crea el header (Rama True)
    colT.extract_hour();
    const initialHeaderCount = df.headers.length;

    // Segunda ejecución: El header ya existe (Rama False)
    colT.extract_hour();
    
    expect(df.headers.length).toBe(initialHeaderCount);
    expect(df.headers.filter(h => h === 't_hour').length).toBe(1);
  });
});
});