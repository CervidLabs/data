import { DataFrame } from '../core/DataFrame.js';
/**
 * StringIndexer - Convierte strings a índices numéricos
 * Similar a sklearn.preprocessing.LabelEncoder
 */
export class StringIndexer {
  constructor(options = {}) {
    this.options = {
      handleUnknown: 'error', // 'error', 'keep', 'useExisting'
      ...options
    };
    this.vocabulary = new Map();
    this.labels = [];
    this.isFitted = false;
  }

  /**
   * Ajusta el indexer con los datos
   * @param {DataFrame} df - DataFrame con los datos
   * @param {string|Array} columns - Columna(s) a indexar
   */
  fit(df, columns) {
    const cols = Array.isArray(columns) ? columns : [columns];
    
    for (const col of cols) {
      const uniqueValues = new Set();
      for (let i = 0; i < df.rowCount; i++) {
        const value = df.columns[col][i];
        if (value !== null && value !== undefined) {
          uniqueValues.add(String(value));
        }
      }
      
      const sorted = Array.from(uniqueValues).sort();
      const colMap = new Map();
      sorted.forEach((label, idx) => {
        colMap.set(label, idx);
      });
      
      this.vocabulary.set(col, colMap);
      this.labels.push({ col, labels: sorted });
    }
    
    this.isFitted = true;
    return this;
  }

  /**
   * Transforma los datos a índices
   * @param {DataFrame} df - DataFrame a transformar
   * @returns {DataFrame} Nuevo DataFrame con columnas indexadas
   */
  transform(df) {
    if (!this.isFitted) {
      throw new Error('StringIndexer must be fitted first');
    }
    
    const newColumns = { ...df.columns };
    
    for (const { col } of this.labels) {
      const colMap = this.vocabulary.get(col);
      const indexedCol = `${col}_indexed`;
      
      newColumns[indexedCol] = [];
      for (let i = 0; i < df.rowCount; i++) {
        const value = df.columns[col][i];
        if (value === null || value === undefined) {
          newColumns[indexedCol].push(-1);
        } else {
          const strValue = String(value);
          if (colMap.has(strValue)) {
            newColumns[indexedCol].push(colMap.get(strValue));
          } else {
            if (this.options.handleUnknown === 'error') {
              throw new Error(`Unknown label: ${strValue}`);
            } else if (this.options.handleUnknown === 'keep') {
              newColumns[indexedCol].push(strValue);
            } else {
              newColumns[indexedCol].push(-1);
            }
          }
        }
      }
    }
    
    return new DataFrame({ 
      columns: newColumns, 
      rowCount: df.rowCount 
    });
  }

  /**
   * Fit + Transform en un solo paso
   */
  fitTransform(df, columns) {
    return this.fit(df, columns).transform(df);
  }

  /**
   * Convierte índice de vuelta a label original
   * @param {string} col - Columna original
   * @param {number} index - Índice a convertir
   * @returns {string} Label original
   */
  inverseTransform(col, index) {
    const colMap = this.vocabulary.get(col);
    if (!colMap) return null;
    
    for (const [label, idx] of colMap.entries()) {
      if (idx === index) return label;
    }
    return null;
  }

  /**
   * Obtiene el vocabulario completo de una columna
   */
  getLabels(col) {
    const entry = this.labels.find(l => l.col === col);
    return entry ? entry.labels : [];
  }
}