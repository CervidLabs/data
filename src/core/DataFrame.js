import fs from 'fs';
import path from 'path';
import { CSVExporter } from '../exporters/csv.js';
import { JSONExporter } from '../exporters/json.js';
import { TXTExporter } from '../exporters/txt.js';

export class DataFrame {
  constructor(config = {}) {
    // Si config es una instancia, extraemos sus datos
    const data = config instanceof DataFrame ? config : config;

    this.columns = data.columns || {};
    this.rowCount = data.rowCount || 0;
    
    // 🚨 REGLA DE ORO: Si hay nuevas columnas en 'columns', 
    // pero no están en 'headers', las sincronizamos.
    this.headers = data.headers || Object.keys(this.columns);

    // Herencia Nitro
    this.originalBuffer = data.originalBuffer || null;
    this.offsets = data.offsets || null;
    this.numCols = data.numCols || 0;
    this.colMap = data.colMap || null;
    this.metadata = data.metadata || { indexers: {} };
  }
  // ⚡ FILTER NITRO: Mantiene el linaje Nitro
  filter(inputs, predicate) {
    const indices = new Int32Array(this.rowCount);
    let count = 0;
    const inputCols = inputs.map(name => {
      if (!this.columns[name]) throw new Error(`Filtro: Columna '${name}' no encontrada`);
      return this.columns[name];
    });

    for (let i = 0; i < this.rowCount; i++) {
      const args = inputCols.map(col => col[i]);
      if (predicate(...args)) indices[count++] = i;
    }

    const newColumns = {};
    for (const h of this.headers) {
      const oldCol = this.columns[h];
      const newCol = new Float64Array(count);
      for (let j = 0; j < count; j++) newCol[j] = oldCol[indices[j]];
      newColumns[h] = newCol;
    }

    // 🚨 Retornamos heredando el contexto Nitro (originalBuffer y offsets)
    return new DataFrame({
      ...this,
      columns: newColumns,
      rowCount: count
    });
  }

  // 🏷️ WITH_LABEL: Sincroniza el Indexer con los bytes originales
  with_label(specs) {
    const newColumns = { ...this.columns };
    const newMetadata = { ...this.metadata, indexers: { ...this.metadata.indexers } };

    for (const spec of specs) {
      const { input, indexer } = spec;
      const targetName = `${input}_indexed`;
      
      // fitTransform usa los offsets para "ver" el texto en el buffer
      const indexedCol = indexer.fitTransform(this, input); 
      
      newColumns[targetName] = indexedCol;
      newMetadata.indexers[input] = indexer;
    }

    return new DataFrame({ ...this, columns: newColumns, metadata: newMetadata });
  }

    // 🔧 WITH_COLUMNS: Feature Engineering de alta velocidad
  with_columns(specs) {
      const newColumns = { ...this.columns };
      const newHeaders = [...this.headers];

      for (const spec of specs) {
        const target = new Float64Array(this.rowCount);
        const inputCols = spec.inputs.map(name => {
          if (!this.columns[name]) throw new Error(`Columna no encontrada: ${name}`);
          return this.columns[name];
        });

        for (let i = 0; i < this.rowCount; i++) {
          const args = inputCols.map(col => col[i]);
          target[i] = spec.formula(...args);
        }

        newColumns[spec.name] = target;
        // 🚨 REGISTRAMOS LA NUEVA COLUMNA EN HEADERS
        if (!newHeaders.includes(spec.name)) newHeaders.push(spec.name);
      }

      // Devolvemos una instancia con TODA la información
      return new DataFrame({
        columns: newColumns,
        headers: newHeaders,
        rowCount: this.rowCount,
        originalBuffer: this.originalBuffer,
        offsets: this.offsets,
        numCols: this.numCols,
        colMap: this.colMap,
        metadata: this.metadata
      });
    }

  async write(path) {
      const stream = fs.createWriteStream(path);
      // Escribir cabeceras
      stream.write(this.headers.join(',') + '\n');

      // Escribir filas
      for (let i = 0; i < this.rowCount; i++) {
          const row = this.headers.map(h => {
              const val = this.columns[h][i];
              // Si es un número muy grande o timestamp, lo dejamos como está
              // Si es un float, limitamos decimales para que el CSV no pese tanto
              return Number.isInteger(val) ? val : val.toFixed(4);
          });
          stream.write(row.join(',') + '\n');
      }
      
      return new Promise((resolve) => stream.on('finish', resolve).end());
  }

  // ==================== AGREGACIONES & UTILIDADES ====================

  groupByRange(colName, targetCol, maxRange) {
    const groupCounts = new Uint32Array(maxRange);
    const groupSums = new Float64Array(maxRange);
    const keys = this.columns[colName];
    const values = this.columns[targetCol];

    if (!keys || !values) return [];

    for (let i = 0; i < this.rowCount; i++) {
        const key = Math.floor(keys[i]);
        // 🛡️ Protección contra IDs fuera de rango (como el 980414)
        if (key >= 0 && key < maxRange) {
            groupCounts[key]++;
            groupSums[key] += values[i];
        }
    }

    return Array.from({ length: maxRange }, (_, i) => ({
        group: i,
        avg: groupCounts[i] > 0 ? groupSums[i] / groupCounts[i] : 0
    })).filter(r => r.avg > 0).sort((a, b) => b.avg - a.avg);
  }

  groupByID(colName, targetCol) {
    return this.groupByRange(colName, targetCol, 300);
  }

  sum(col) {
    const data = this.columns[col];
    if (!data) return 0;
    let total = 0;
    for (let i = 0; i < this.rowCount; i++) total += data[i];
    return total;
  }

  mean(col) {
    return this.rowCount === 0 ? 0 : this.sum(col) / this.rowCount;
  }

  max(col) {
    const data = this.getCol(col);
    if (!data) return null;
    let maxVal = -Infinity;
    const len = this.rowCount;
    for (let i = 0; i < len; i++) {
      if (data[i] > maxVal) maxVal = data[i];
    }
    return maxVal;
  }

  min(col) {
    const data = this.getCol(col);
    if (!data) return null;
    let minVal = Infinity;
    const len = this.rowCount;
    for (let i = 0; i < len; i++) {
      if (data[i] < minVal) minVal = data[i];
    }
    return minVal;
  }

  // ==================== UTILIDADES & EXPORTACIÓN ====================

  info() {
    return {
      rowCount: this.rowCount,
      columnCount: this.headers.length,
      columns: this.headers,
      memoryUsage: `${((this.rowCount * this.headers.length * 8) / 1024 / 1024).toFixed(2)} MB`
    };
  }

  fromArray(data) {
    if (!data || data.length === 0) return this;
    this.headers = Object.keys(data[0]);
    this.rowCount = data.length;
    
    this.headers.forEach(h => {
      this.columns[h] = new Float64Array(new SharedArrayBuffer(this.rowCount * 8));
      for (let i = 0; i < this.rowCount; i++) {
        this.columns[h][i] = data[i][h];
      }
    });
    return this;
  }

  toArray() {
    const result = [];
    for (let i = 0; i < this.rowCount; i++) {
      result.push(this._getRow(i));
    }
    return result;
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