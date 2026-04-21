import fs from 'fs';
import path from 'path';
// Asumimos que los exportadores y Column también migrarán a .ts pronto
import { type CSVExporterOptions, CSVExporter } from '../exporters/csv.js';
import { type JSONExporterOptions, JSONExporter } from '../exporters/json.js';
import { type TXTExporterOptions, TXTExporter } from '../exporters/txt.js';
import { Column } from './Column.js';
interface GroupAccumulator {
  sum: number;
  count: number;
}
// ==========================================
// 🛡️ INTERFACES Y TIPOS MILITARIZADOS
// ==========================================

export type SupportedTypedArray = Float32Array | Float64Array | Int8Array | Int16Array | Int32Array | Uint8Array | Uint16Array | Uint32Array;

// Una columna puede ser un TypedArray de alto rendimiento o un Array estándar de JS
export type ColumnData = SupportedTypedArray | unknown[];

export interface DataFrameMetadata {
  indexers?: Record<string, unknown>;
  source?: string;
  shape?: number[];
  [key: string]: unknown; // Permite metadatos extra
}

export interface DataFrameConfig {
  columns?: Record<string, ColumnData>;
  rowCount?: number;
  headers?: string[];
  filePath?: string;
  fileType?: string;
  originalBuffer?: SharedArrayBuffer | ArrayBuffer | null;
  numCols?: number;
  offsets?: number[] | Int32Array | null;
  colMap?: Record<string, number> | null;
  metadata?: DataFrameMetadata;
}

export interface SharedSchemaCol {
  name: string;
  dtype: string;
  offset: number;
  length: number;
}

export interface SharedDef {
  schema: SharedSchemaCol[];
  buffer: SharedArrayBuffer | ArrayBuffer;
  rowCount: number;
  source: string;
  shape: number[];
  metadata?: Record<string, unknown>;
}

export interface ColumnSpec {
  name: string;
  inputs: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  formula: (...args: any[]) => any;
}

export interface LabelSpec {
  input: string;
  indexer: {
    fitTransform: (df: DataFrame, input: string) => ColumnData;
  };
}

// ==========================================
// 🚀 CLASE DATAFRAME
// ==========================================

export class DataFrame {
  public columns: Record<string, ColumnData>;
  public rowCount: number;
  public headers: string[];

  // Propiedades dinámicas asignadas por los readers
  public filePath?: string;
  public fileType?: string;

  // Herencia Nitro
  public originalBuffer: SharedArrayBuffer | ArrayBuffer | null;
  public offsets: number[] | Int32Array | null;
  public numCols: number;
  public colMap: Record<string, number> | null;
  public metadata: DataFrameMetadata;

  constructor(config: DataFrameConfig | DataFrame = {}) {
    const data = config instanceof DataFrame ? config : config;

    this.columns = data.columns ?? {};
    this.rowCount = data.rowCount ?? 0;
    this.headers = data.headers ?? Object.keys(this.columns);

    this.originalBuffer = data.originalBuffer ?? null;
    this.offsets = data.offsets ?? null;
    this.numCols = data.numCols ?? 0;
    this.colMap = data.colMap ?? null;
    this.metadata = data.metadata ?? { indexers: {} };

    // Si viene de otra instancia, copiamos propiedades extra
    if (data.filePath) {
      this.filePath = data.filePath;
    }
    if (data.fileType) {
      this.fileType = data.fileType;
    }
  }

  static fromShared(def: SharedDef): DataFrame {
    const columns: Record<string, ColumnData> = {};
    const headers: string[] = [];

    for (const col of def.schema) {
      const TypedArrayConstructor = this._dtypeToTypedArray(col.dtype);
      // Construimos la vista directamente sobre el buffer
      columns[col.name] = new TypedArrayConstructor(def.buffer, col.offset, col.length);
      headers.push(col.name);
    }

    return new DataFrame({
      columns,
      rowCount: def.rowCount,
      headers,
      metadata: {
        ...def.metadata,
        source: def.source,
        shape: def.shape,
      },
      originalBuffer: def.buffer,
    });
  }

  // Define estrictamente el tipo de constructores permitidos
  private static _dtypeToTypedArray(dtype: string): new (buffer: ArrayBufferLike, byteOffset: number, length: number) => SupportedTypedArray {
    const map: Record<string, new (buffer: ArrayBufferLike, byteOffset: number, length: number) => SupportedTypedArray> = {
      f4: Float32Array,
      f8: Float64Array,
      i1: Int8Array,
      i2: Int16Array,
      i4: Int32Array,
      u1: Uint8Array,
      u2: Uint16Array,
      u4: Uint32Array,
      b1: Uint8Array, // En TS, tratamos el b1 (boolean) como Uint8Array en memoria plana
    };

    const clean = dtype.replace(/[<>=|]/g, '');
    const TA = map[clean];

    if (!TA) {
      throw new Error(`Unsupported dtype: ${dtype}`);
    }
    return TA;
  }

  static fromObjects(data: Record<string, unknown>[]): DataFrame {
    if (!data || data.length === 0) {
      return new DataFrame({ columns: {}, rowCount: 0, headers: [] });
    }

    const headers = Object.keys(data[0]);
    const columns: Record<string, ColumnData> = {};
    const rowCount = data.length;

    headers.forEach((h) => {
      const sample = data[0][h];

      if (typeof sample === 'number' || sample === null || sample === undefined) {
        const col = new Float64Array(rowCount);

        for (let i = 0; i < rowCount; i++) {
          const value = data[i][h];
          col[i] = value === null || value === undefined ? Number.NaN : Number(value);
        }

        columns[h] = col;
      } else {
        columns[h] = data.map((row) => row[h]);
      }
    });

    return new DataFrame({ columns, rowCount, headers });
  }

  with_label(specs: LabelSpec[]): DataFrame {
    const newColumns = { ...this.columns };
    const newHeaders = [...this.headers];
    const newMetadata = {
      ...this.metadata,
      indexers: { ...(this.metadata.indexers ?? {}) },
    };

    for (const spec of specs) {
      const { input, indexer } = spec;
      const targetName = `${input}_indexed`;
      const indexedCol = indexer.fitTransform(this, input);

      newColumns[targetName] = indexedCol;

      if (!newHeaders.includes(targetName)) {
        newHeaders.push(targetName);
      }

      (newMetadata.indexers as Record<string, unknown>)[input] = indexer;
    }

    return new DataFrame({
      ...this,
      columns: newColumns,
      headers: newHeaders,
      metadata: newMetadata,
    });
  }
  with_columns(specs: ColumnSpec[]): DataFrame {
    const rowCount = this.rowCount;
    for (const spec of specs) {
      const newCol = new Float64Array(rowCount);
      const formula = spec.formula;
      const inputs = spec.inputs.map((name) => this.columns[name]);
      const numInputs = inputs.length;

      if (numInputs === 1) {
        const col0 = inputs[0] as ArrayLike<number>;
        for (let i = 0; i < rowCount; i++) {
          newCol[i] = formula(col0[i]);
        }
      } else if (numInputs === 2) {
        const col0 = inputs[0] as ArrayLike<number>;
        const col1 = inputs[1] as ArrayLike<number>;
        for (let i = 0; i < rowCount; i++) {
          newCol[i] = formula(col0[i], col1[i]);
        }
      } else if (numInputs === 4) {
        const col0 = inputs[0] as ArrayLike<number>;
        const col1 = inputs[1] as ArrayLike<number>;
        const col2 = inputs[2] as ArrayLike<number>;
        const col3 = inputs[3] as ArrayLike<number>;
        for (let i = 0; i < rowCount; i++) {
          newCol[i] = formula(col0[i], col1[i], col2[i], col3[i]);
        }
      } else {
        for (let i = 0; i < rowCount; i++) {
          const args = new Array(numInputs);
          for (let j = 0; j < numInputs; j++) {
            args[j] = (inputs[j] as ArrayLike<unknown>)[i];
          }
          newCol[i] = formula(...args);
        }
      }
      this.columns[spec.name] = newCol;
      if (!this.headers.includes(spec.name)) {
        this.headers.push(spec.name);
      }
    }
    return this;
  }

  show(n = 5): void {
    const limit = Math.min(n, this.rowCount);
    const tableData = [];

    for (let i = 0; i < limit; i++) {
      const row: Record<string, unknown> = {};
      for (const header of this.headers) {
        let val = (this.columns[header] as ArrayLike<unknown>)[i];
        if (typeof val === 'string' && val.length > 20) {
          val = val.substring(0, 17) + '...';
        }
        row[header] = val;
      }
      tableData.push(row);
    }

    console.table(tableData);
  }

  async write(filePath: string): Promise<void> {
    const stream = fs.createWriteStream(filePath);
    stream.write(this.headers.join(',') + '\n');

    for (let i = 0; i < this.rowCount; i++) {
      const row = this.headers.map((h) => {
        const val = (this.columns[h] as ArrayLike<unknown>)[i];
        return typeof val === 'number' && !Number.isInteger(val) ? val.toFixed(4) : String(val);
      });
      stream.write(row.join(',') + '\n');
    }

    return new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
      stream.end();
    });
  }

  // ==================== AGREGACIONES & UTILIDADES ====================
  public groupByCategory(groupColumn: string, valueColumn: string): Map<string | number, GroupAccumulator> {
    const groups = this.columns[groupColumn];
    const values = this.columns[valueColumn];

    if (groups === undefined || values === undefined) {
      throw new Error(`Column not found: ${groups === undefined ? groupColumn : valueColumn}`);
    }

    // Tipamos el Map explícitamente para evitar 'any'
    const acc = new Map<string | number, GroupAccumulator>();

    for (let i = 0; i < this.rowCount; i++) {
      const group = groups[i] as string | number;
      const value = values[i];

      // Check de existencia rápido
      if (group === null || group === undefined || group === '') {
        continue;
      }

      // Validación numérica estricta
      if (typeof value !== 'number' || isNaN(value)) {
        continue;
      }

      const current = acc.get(group);

      if (current !== undefined) {
        // TypeScript ya sabe que 'current' es GroupAccumulator gracias al check !== undefined
        current.sum += value;
        current.count += 1;
      } else {
        acc.set(group, { sum: value, count: 1 });
      }
    }

    return acc;
  }
  groupByRange(colName: string, targetCol: string, maxRange: number): { group: number; avg: number }[] {
    const groupCounts = new Uint32Array(maxRange);
    const groupSums = new Float64Array(maxRange);
    const keys = this.columns[colName] as ArrayLike<number>;
    const values = this.columns[targetCol] as ArrayLike<number>;

    if (!keys || !values) {
      return [];
    }

    for (let i = 0; i < this.rowCount; i++) {
      const key = Math.floor(keys[i]);
      if (key >= 0 && key < maxRange) {
        groupCounts[key]++;
        groupSums[key] += values[i];
      }
    }

    return Array.from({ length: maxRange }, (_, i) => ({
      group: i,
      avg: groupCounts[i] > 0 ? groupSums[i] / groupCounts[i] : 0,
    }))
      .filter((r) => r.avg > 0)
      .sort((a, b) => b.avg - a.avg);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  filter(inputs: string[], predicate: (...args: any[]) => boolean): DataFrame {
    const indices: number[] = [];

    for (let i = 0; i < this.rowCount; i++) {
      const rowInputs = inputs.map((h) => (this.columns[h] as ArrayLike<unknown>)[i]);
      if (predicate(...rowInputs)) {
        indices.push(i);
      }
    }

    const newColumns: Record<string, ColumnData> = {};
    for (const h of this.headers) {
      const oldCol = this.columns[h];
      const isTyped = oldCol instanceof Float64Array;

      const newCol = isTyped ? new Float64Array(indices.length) : new Array(indices.length);

      for (let j = 0; j < indices.length; j++) {
        newCol[j] = (oldCol as ArrayLike<unknown>)[indices[j]];
      }
      newColumns[h] = newCol as ColumnData;
    }

    return new DataFrame({
      ...this,
      columns: newColumns,
      rowCount: indices.length,
    });
  }

  groupBy(groupCol: string, aggs: Record<string, string | string[]> = {}): DataFrame {
    const groups = new Map<unknown, number[]>();
    const targetData = this.columns[groupCol] as ArrayLike<unknown>;

    for (let i = 0; i < this.rowCount; i++) {
      const val = targetData[i];
      if (!groups.has(val)) {
        groups.set(val, []);
      }
      groups.get(val)!.push(i);
    }

    const resultRows: Record<string, unknown>[] = [];
    for (const [groupVal, indices] of groups.entries()) {
      const row: Record<string, unknown> = { [groupCol]: groupVal };

      for (const [colName, ops] of Object.entries(aggs)) {
        const colToAgg = this.columns[colName] as ArrayLike<unknown>;
        const values = indices.map((idx) => parseFloat(colToAgg[idx] as string)).filter((v) => !Number.isNaN(v));

        const operations = Array.isArray(ops) ? ops : [ops];

        operations.forEach((op) => {
          const outName = operations.length > 1 ? `${colName}_${op}` : colName;

          if (op === 'sum') {
            row[outName] = values.reduce((a, b) => a + b, 0);
          } else if (op === 'mean') {
            row[outName] = values.reduce((a, b) => a + b, 0) / values.length;
          } else if (op === 'count') {
            row[outName] = values.length;
          } else if (op === 'max') {
            row[outName] = Math.max(...values);
          } else if (op === 'min') {
            row[outName] = Math.min(...values);
          }
        });
      }
      resultRows.push(row);
    }

    return DataFrame.fromObjects(resultRows);
  }

  sort(columnName: string, ascending = true): DataFrame {
    const indices = Array.from({ length: this.rowCount }, (_, i) => i);
    const targetCol = this.columns[columnName] as ArrayLike<number | string>;

    indices.sort((a, b) => {
      const valA = targetCol[a];
      const valB = targetCol[b];

      if (valA < valB) {
        return ascending ? -1 : 1;
      }
      if (valA > valB) {
        return ascending ? 1 : -1;
      }
      return 0;
    });

    const newColumns: Record<string, ColumnData> = {};
    for (const h of this.headers) {
      const oldCol = this.columns[h];
      const isTyped = oldCol instanceof Float64Array;
      const newCol = isTyped ? new Float64Array(this.rowCount) : new Array(this.rowCount);

      for (let j = 0; j < this.rowCount; j++) {
        newCol[j] = (oldCol as ArrayLike<unknown>)[indices[j]];
      }
      newColumns[h] = newCol as ColumnData;
    }

    return new DataFrame({ ...this, columns: newColumns });
  }

  groupByID(colName: string, targetCol: string): { group: number; avg: number }[] {
    return this.groupByRange(colName, targetCol, 300);
  }

  sum(col: string): number {
    const data = this.columns[col] as ArrayLike<number>;
    if (!data) {
      return 0;
    }
    let total = 0;
    for (let i = 0; i < this.rowCount; i++) {
      total += data[i];
    }
    return total;
  }

  mean(col: string): number {
    return this.rowCount === 0 ? 0 : this.sum(col) / this.rowCount;
  }

  max(col: string): number | null {
    const data = this.columns[col] as ArrayLike<number>;
    if (!data) {
      return null;
    }
    let maxVal = -Infinity;
    const len = this.rowCount;
    for (let i = 0; i < len; i++) {
      if (data[i] > maxVal) {
        maxVal = data[i];
      }
    }
    return maxVal;
  }

  min(col: string): number | null {
    const data = this.columns[col] as ArrayLike<number>;
    if (!data) {
      return null;
    }
    let minVal = Infinity;
    const len = this.rowCount;
    for (let i = 0; i < len; i++) {
      if (data[i] < minVal) {
        minVal = data[i];
      }
    }
    return minVal;
  }

  stats(colName: string): { count: number; sum: number; mean: number; min: number | null; max: number | null } | null {
    const col = this.columns[colName] as ArrayLike<number>;
    if (!col) {
      return null;
    }

    const len = this.rowCount;
    if (len === 0) {
      return { count: 0, sum: 0, mean: 0, min: null, max: null };
    }

    let sum = 0;
    let min = Infinity;
    let max = -Infinity;

    for (let i = 0; i < len; i++) {
      const v = col[i];
      sum += v;
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }

    return { count: len, sum, mean: sum / len, min, max };
  }

  // ==================== UTILIDADES & EXPORTACIÓN ====================

  info(): Record<string, unknown> {
    return {
      rowCount: this.rowCount,
      columnCount: this.headers.length,
      columns: this.headers,
      memoryUsage: `${((this.rowCount * this.headers.length * 8) / 1024 / 1024).toFixed(2)} MB`,
    };
  }

  getCol(name: string): ColumnData | null {
    return this.columns[name] || null;
  }

  fromArray(data: Record<string, number>[]): this {
    if (!data || data.length === 0) {
      return this;
    }
    this.headers = Object.keys(data[0]);
    this.rowCount = data.length;

    this.headers.forEach((h) => {
      this.columns[h] = new Float64Array(new SharedArrayBuffer(this.rowCount * 8));
      for (let i = 0; i < this.rowCount; i++) {
        (this.columns[h] as Float64Array)[i] = data[i][h];
      }
    });
    return this;
  }

  private _getRow(index: number): Record<string, unknown> {
    const row: Record<string, unknown> = {};
    for (const h of this.headers) {
      row[h] = (this.columns[h] as ArrayLike<unknown>)[index];
    }
    return row;
  }

  toArray(): Record<string, unknown>[] {
    const result = [];
    for (let i = 0; i < this.rowCount; i++) {
      result.push(this._getRow(i));
    }
    return result;
  }

  private _validatePath(outputPath: string, requiredExt: string): string {
    const ext = path.extname(outputPath).toLowerCase();
    if (!ext) {
      return outputPath + requiredExt;
    }
    if (ext !== requiredExt) {
      throw new Error(`Invalid extension: Output must be ${requiredExt} (received: ${ext})`);
    }
    return outputPath;
  }
  /**
   * Exporta a CSV con validación estricta de opciones
   */
  async toCSV(outputPath: string, options: CSVExporterOptions = {}): Promise<void> {
    const validatedPath = this._validatePath(outputPath, '.csv');
    const exporter = new CSVExporter(this, options);
    await exporter.export(validatedPath);
  }

  /**
   * Exporta a JSON/NDJSON
   */
  async toJSON(outputPath: string, options: JSONExporterOptions = {}): Promise<void> {
    const validatedPath = this._validatePath(outputPath, '.json');
    const exporter = new JSONExporter(this, options);
    await exporter.export(validatedPath);
  }

  /**
   * Exporta a TXT (Reporte visual)
   */
  async toTXT(outputPath: string, options: TXTExporterOptions = {}): Promise<void> {
    const validatedPath = this._validatePath(outputPath, '.txt');
    const exporter = new TXTExporter(this, options);
    await exporter.export(validatedPath);
  }
  describe(): void {
    const stats = [];
    for (const h of this.headers) {
      const col = this.columns[h] as ArrayLike<unknown>;

      // Optimización: Usamos un bucle en lugar de Array.from().map().filter() para no colapsar la RAM
      const numericValues: number[] = [];
      for (let i = 0; i < this.rowCount; i++) {
        const val = parseFloat(String(col[i]));
        if (!Number.isNaN(val)) {
          numericValues.push(val);
        }
      }

      if (numericValues.length === 0) {
        continue;
      }

      const sorted = numericValues.sort((a, b) => a - b);
      const count = sorted.length;
      const sum = sorted.reduce((a, b) => a + b, 0);
      const mean = sum / count;

      stats.push({
        column: h,
        count,
        mean: mean.toFixed(2),
        min: sorted[0],
        '25%': sorted[Math.floor(count * 0.25)],
        '50%': sorted[Math.floor(count * 0.5)],
        '75%': sorted[Math.floor(count * 0.75)],
        max: sorted[count - 1],
      });
    }

    if (stats.length === 0) {
      console.info('No numeric columns found to describe.');
    } else {
      console.table(stats);
    }
  }

  rename(mapping: Record<string, string>): DataFrame {
    const newColumns: Record<string, ColumnData> = {};
    const newHeaders = this.headers.map((h) => {
      const newName = mapping[h] || h;
      newColumns[newName] = this.columns[h];
      return newName;
    });

    return new DataFrame({
      ...this,
      columns: newColumns,
      headers: newHeaders,
      colMap: Object.fromEntries(newHeaders.map((h, i) => [h, i])),
    });
  }

  head(n = 5): DataFrame {
    const limit = Math.min(n, this.rowCount);
    const newColumns: Record<string, ColumnData> = {};

    for (const h of this.headers) {
      newColumns[h] = (this.columns[h] as unknown[]).slice(0, limit) as ColumnData;
    }

    return new DataFrame({ ...this, columns: newColumns, rowCount: limit });
  }

  tail(n = 5): DataFrame {
    const start = Math.max(0, this.rowCount - n);
    const newColumns: Record<string, ColumnData> = {};

    for (const h of this.headers) {
      newColumns[h] = (this.columns[h] as unknown[]).slice(start, this.rowCount) as ColumnData;
    }

    return new DataFrame({
      ...this,
      columns: newColumns,
      rowCount: this.rowCount - start,
    });
  }

  unique(columnName: string): unknown[] {
    // Array.from evita problemas con iteradores en TS estricto
    return Array.from(new Set(this.columns[columnName] as Iterable<unknown>));
  }

  nunique(columnName: string): number {
    return new Set(this.columns[columnName] as Iterable<unknown>).size;
  }

  value_counts(columnName: string): { value: string; count: number }[] {
    const counts: Record<string, number> = {};
    const col = this.columns[columnName] as ArrayLike<unknown>;

    for (let i = 0; i < this.rowCount; i++) {
      const val = String(col[i]);
      counts[val] = (counts[val] || 0) + 1;
    }

    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .map(([value, count]) => ({ value, count }));
  }
  dropNA(options: { how?: 'any' | 'all' } = {}): DataFrame {
    const how = options.how ?? 'any';
    const indices: number[] = [];

    for (let i = 0; i < this.rowCount; i++) {
      let nullCount = 0;

      for (const h of this.headers) {
        const val = (this.columns[h] as ArrayLike<unknown>)[i];

        const isNullish = val === null || val === undefined || (typeof val === 'number' && Number.isNaN(val));

        if (isNullish) {
          nullCount++;
        }
      }

      if ((how === 'any' && nullCount === 0) || (how === 'all' && nullCount < this.headers.length)) {
        indices.push(i);
      }
    }

    return this._rebuildFromIndices(indices);
  }
  fillna(value: unknown): DataFrame {
    for (const h of this.headers) {
      const col = this.columns[h] as Array<unknown>; // Solo aplicable si es array normal o si value es numérico
      for (let i = 0; i < this.rowCount; i++) {
        if (col[i] === null || col[i] === undefined || (typeof col[i] === 'number' && Number.isNaN(col[i]))) {
          col[i] = value;
        }
      }
    }
    return this;
  }

  private _rebuildFromIndices(indices: number[]): DataFrame {
    const newColumns: Record<string, ColumnData> = {};
    for (const h of this.headers) {
      const oldCol = this.columns[h];
      const isTyped = oldCol instanceof Float64Array;
      const newCol = isTyped ? new Float64Array(indices.length) : new Array(indices.length);

      for (let j = 0; j < indices.length; j++) {
        newCol[j] = (oldCol as ArrayLike<unknown>)[indices[j]];
      }
      newColumns[h] = newCol as ColumnData;
    }
    return new DataFrame({
      ...this,
      columns: newColumns,
      rowCount: indices.length,
    });
  }

  select(columnNames: string[]): DataFrame {
    const newColumns: Record<string, ColumnData> = {};
    for (const name of columnNames) {
      if (this.columns[name]) {
        newColumns[name] = this.columns[name];
      }
    }
    return new DataFrame({
      ...this,
      columns: newColumns,
      headers: columnNames,
      rowCount: this.rowCount,
    });
  }

  str_contains(columnName: string, pattern: string): DataFrame {
    const regex = new RegExp(pattern, 'i');
    const indices: number[] = [];
    const col = this.columns[columnName] as ArrayLike<string>;

    for (let i = 0; i < this.rowCount; i++) {
      if (col[i] && regex.test(col[i])) {
        indices.push(i);
      }
    }
    return this._rebuildFromIndices(indices);
  }

  cast(columnName: string, type: 'float' | 'int' | 'string'): DataFrame {
    const oldCol = this.columns[columnName] as ArrayLike<unknown>;
    let newCol: ColumnData;

    if (type === 'float' || type === 'int') {
      newCol = new Float64Array(this.rowCount);
      for (let i = 0; i < this.rowCount; i++) {
        newCol[i] = parseFloat(String(oldCol[i])) || 0;
      }
    } else {
      newCol = new Array(this.rowCount);
      for (let i = 0; i < this.rowCount; i++) {
        newCol[i] = String(oldCol[i]);
      }
    }

    this.columns[columnName] = newCol;
    return this;
  }

  cumsum(columnName: string): DataFrame {
    const col = this.columns[columnName] as ArrayLike<unknown>;
    const newCol = new Float64Array(this.rowCount);
    let acc = 0;

    for (let i = 0; i < this.rowCount; i++) {
      acc += parseFloat(String(col[i])) || 0;
      newCol[i] = acc;
    }

    const newName = `${columnName}_cumsum`;
    this.columns[newName] = newCol;
    if (!this.headers.includes(newName)) {
      this.headers.push(newName);
    }

    return this;
  }

  join(other: DataFrame, on: string, how: 'inner' | 'left' = 'inner'): DataFrame {
    const leftCol = this.columns[on] as ArrayLike<unknown>;
    const rightCol = other.columns[on] as ArrayLike<unknown>;

    const rightMap = new Map<unknown, number[]>();
    for (let i = 0; i < other.rowCount; i++) {
      const val = rightCol[i];
      if (!rightMap.has(val)) {
        rightMap.set(val, []);
      }
      rightMap.get(val)!.push(i);
    }

    const joinedRows: Record<string, unknown>[] = [];
    const rightHeaders = other.headers.filter((h) => h !== on);

    for (let i = 0; i < this.rowCount; i++) {
      const leftVal = leftCol[i];
      const matches = rightMap.get(leftVal);

      if (matches) {
        for (const rightIdx of matches) {
          const newRow: Record<string, unknown> = {};
          this.headers.forEach((h) => (newRow[h] = (this.columns[h] as ArrayLike<unknown>)[i]));
          rightHeaders.forEach((h) => (newRow[h] = (other.columns[h] as ArrayLike<unknown>)[rightIdx]));
          joinedRows.push(newRow);
        }
      } else if (how === 'left') {
        const newRow: Record<string, unknown> = {};
        this.headers.forEach((h) => (newRow[h] = (this.columns[h] as ArrayLike<unknown>)[i]));
        rightHeaders.forEach((h) => (newRow[h] = null));
        joinedRows.push(newRow);
      }
    }

    return DataFrame.fromObjects(joinedRows);
  }

  col(name: string): Column {
    if (!this.columns[name]) {
      throw new Error(`Column ${name} not found`);
    }
    return new Column(name, this.columns[name], this);
  }
  /**
   * Asigna nuevas columnas al DataFrame o sobrescribe las existentes.
   * @param data Un objeto con nuevas columnas o un DataFrame completo.
   */
  public assign(data: Record<string, ColumnData> | DataFrame): this {
    const newCols = data instanceof DataFrame ? data.columns : data;

    for (const [colName, colValues] of Object.entries(newCols)) {
      // Verificación de Grado Militar: Evitar desalineación de filas
      if (colValues.length !== this.rowCount && this.rowCount !== 0) {
        throw new Error(`Error de integridad: La columna '${colName}' tiene ${colValues.length} filas, pero el DataFrame tiene ${this.rowCount}.`);
      }

      // Si el DataFrame estaba vacío, heredamos el rowCount de la primera columna
      if (this.rowCount === 0) {
        this.rowCount = colValues.length;
      }

      // Asignación directa
      this.columns[colName] = colValues;

      // Actualizar el header si la columna es nueva
      if (!this.headers.includes(colName)) {
        this.headers.push(colName);
      }
    }

    return this;
  }
}
