import type { DataFrame } from '../core/DataFrame.js';

export interface StringIndexerOptions {
  handleUnknown?: 'keep' | 'error';
}

/**
 * StringIndexer - Codifica columnas de texto a índices numéricos
 */
export class StringIndexer {
  private options: Required<StringIndexerOptions>;
  private maps: Record<string, Map<string, number>>;
  private labels: Record<string, string[]>;
  private decoder: InstanceType<typeof TextDecoder>; // Simplificado el tipo
  private isFitted: boolean;

  constructor(options: StringIndexerOptions = {}) {
    this.options = {
      handleUnknown: 'keep',
      ...options,
    };
    this.maps = {};
    this.labels = {};
    this.decoder = new TextDecoder();
    this.isFitted = false;
  }

  public fitTransform(df: DataFrame, input: string | string[]): Float64Array {
    const colName = Array.isArray(input) ? input[0] : input;

    const distinctValues = new Set<string>();
    const map = new Map<string, number>();
    const labels: string[] = [];

    for (let i = 0; i < df.rowCount; i++) {
      const val = this._extract(df, colName, i);
      distinctValues.add(val);
    }

    Array.from(distinctValues)
      .sort()
      .forEach((val, idx) => {
        map.set(val, idx);
        labels.push(val);
      });

    this.maps[colName] = map;
    this.labels[colName] = labels;
    this.isFitted = true;

    const result = new Float64Array(df.rowCount);
    for (let i = 0; i < df.rowCount; i++) {
      const val = this._extract(df, colName, i);
      result[i] = map.get(val) ?? labels.length;
    }

    return result;
  }

  private _extract(df: DataFrame, colName: string, rowIdx: number): string {
    // Solución a warning 76 y 97: Check explícito contra undefined/null
    if (df.colMap === undefined || df.colMap === null) {
      throw new Error('DataFrame no tiene colMap.');
    }
    const colIdx = df.colMap[colName];
    // Warning 76: colIdx puede ser 0 (falsy). Check explícito.
    if (colIdx === undefined || colIdx === null) {
      throw new Error(`Columna "${colName}" no encontrada.`);
    }
    if (df.offsets === undefined || df.offsets === null) {
      throw new Error('DataFrame no tiene offsets cargados.');
    }
    // Cálculo del puntero
    const numCols = Object.keys(df.colMap).length;
    const offIdx = (rowIdx * numCols + colIdx) * 2;
    const start = df.offsets[offIdx];
    const end = df.offsets[offIdx + 1];

    if (df.originalBuffer === undefined || df.originalBuffer === null) {
      throw new Error('El buffer original es inaccesible.');
    }
    return this.decoder.decode(new Uint8Array(df.originalBuffer, start, end - start)).trim();
  }

  public getLabels(colName?: string): string[] {
    // Warning 110: Check explícito de string vacío o nulo
    const target = colName !== undefined && colName !== '' ? colName : Object.keys(this.labels)[0];
    if (target === undefined || !this.labels[target]) {
      throw new Error(`StringIndexer: No hay etiquetas disponibles para "${target}"`);
    }
    return this.labels[target];
  }

  public getIndex(colName: string, label?: string): number {
    if (label === undefined) {
      const firstCol = Object.keys(this.maps)[0];
      if (firstCol === undefined) {
        return -1;
      }
      return this.maps[firstCol].get(colName) ?? -1;
    }

    const map = this.maps[colName];
    // Warning 115: Check explícito de existencia del map
    if (map === undefined) {
      return -1;
    }
    return map.get(label) ?? -1;
  }
}
