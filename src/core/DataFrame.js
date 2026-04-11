import fs from 'fs';
import path from 'path';
import { CSVExporter } from '../exporters/csv.js';
import { JSONExporter } from '../exporters/json.js';
import { TXTExporter } from '../exporters/txt.js';

export class DataFrame {
  constructor(source = null, options = {}) {
    this.columns = {};
    this.rowCount = 0;
    this.headers = [];
    this.options = {
      parallel: true,
      lazy: false,
      ...options
    };

    if (source) {
      // ✅ CASO NITRO: El ParallelExecutor devuelve { columns, rowCount }
      if (source.columns && source.rowCount !== undefined) {
        this.columns = source.columns;
        this.rowCount = source.rowCount;
        this.headers = Object.keys(source.columns);
      } 
      // CASO ARRAY: Fallback para datos cargados manualmente
      else if (Array.isArray(source)) {
        this.fromArray(source);
      } 
      // CASO OBJETO SIMPLE
      else if (typeof source === 'object' && source.columns) {
        this.columns = source.columns;
        this.headers = Object.keys(source.columns);
        this.rowCount = source.rowCount || (this.headers.length > 0 ? this.columns[this.headers[0]].length : 0);
      }
    }
  }

  // ==================== AGREGACIONES NITRO (Operan sobre TypedArrays) ====================

  sum(col) {
    const data = this.columns[col];
    if (!data) return 0;
    let total = 0;
    const len = this.rowCount;
    for (let i = 0; i < len; i++) {
      total += data[i];
    }
    return total;
  }

  mean(col) {
    if (this.rowCount === 0) return 0;
    return this.sum(col) / this.rowCount;
  }

  max(col) {
    const data = this.columns[col];
    if (!data) return null;
    let maxVal = -Infinity;
    const len = this.rowCount;
    for (let i = 0; i < len; i++) {
      if (data[i] > maxVal) maxVal = data[i];
    }
    return maxVal;
  }

  min(col) {
    const data = this.columns[col];
    if (!data) return null;
    let minVal = Infinity;
    const len = this.rowCount;
    for (let i = 0; i < len; i++) {
      if (data[i] < minVal) minVal = data[i];
    }
    return minVal;
  }

  // ==================== TRANSFORMACIONES ====================

  /**
   * Crea una nueva columna basada en una función.
   * Optimizado para SharedArrayBuffers.
   */
  assign(colName, fn) {
    const newCol = new Float64Array(new SharedArrayBuffer(this.rowCount * 8));
    const len = this.rowCount;
    
    for (let i = 0; i < len; i++) {
      // Pasamos un objeto proxy ligero de la fila actual
      newCol[i] = fn(this._getRow(i));
    }
    
    this.columns[colName] = newCol;
    if (!this.headers.includes(colName)) this.headers.push(colName);
    return this;
  }

  // ==================== INSPECCIÓN ====================

  head(n = 5) {
    const result = [];
    const limit = Math.min(n, this.rowCount);
    for (let i = 0; i < limit; i++) {
      result.push(this._getRow(i));
    }
    return result;
  }

  _getRow(i) {
    const row = {};
    for (const h of this.headers) {
      row[h] = this.columns[h][i];
    }
    return row;
  }

  info() {
    return {
      rowCount: this.rowCount,
      columnCount: this.headers.length,
      columns: this.headers,
      memoryUsage: `${((this.rowCount * this.headers.length * 8) / 1024 / 1024).toFixed(2)} MB`
    };
  }

  // ==================== FILTRADO NITRO ====================

  filter(predicate) {
    const indices = [];
    const len = this.rowCount;
    
    for (let i = 0; i < len; i++) {
      if (predicate(this._getRow(i), i)) {
        indices.push(i);
      }
    }
    
    return this._takeIndices(indices);
  }

  _takeIndices(indices) {
    const newColumns = {};
    const newRowCount = indices.length;

    for (const h of this.headers) {
      const oldCol = this.columns[h];
      const newCol = new Float64Array(new SharedArrayBuffer(newRowCount * 8));
      for (let i = 0; i < newRowCount; i++) {
        newCol[i] = oldCol[indices[i]];
      }
      newColumns[h] = newCol;
    }

    return new DataFrame({ columns: newColumns, rowCount: newRowCount });
  }

  // ==================== CARGA / EXPORTACIÓN ====================

  fromArray(data) {
    if (!data || data.length === 0) return this;
    this.headers = Object.keys(data[0]);
    this.rowCount = data.length;
    
    this.headers.forEach(h => {
      this.columns[h] = new Float64Array(this.rowCount);
      for (let i = 0; i < this.rowCount; i++) {
        this.columns[h][i] = data[i][h];
      }
    });
    return this;
  }

  async toCSV(outputPath, options = {}) {
    const exporter = new CSVExporter(this, options);
    return await exporter.export(outputPath);
  }
  async toJSON(outputPath, options = {}) {
    const exporter = new JSONExporter(this, options);
    return await exporter.export(outputPath);
  }
  async toTXT(outputPath, options = {}) {
    const exporter = new TXTExporter(this, options);
    return await exporter.export(outputPath);
  } 
}