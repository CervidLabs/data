import { DataFrame } from '../core/DataFrame.js';

export class StringIndexer {
  constructor(options = {}) {
    this.options = {
      handleUnknown: 'error', 
      ...options
    };
    this.vocabulary = new Map(); 
    this.inverseVocabulary = new Map(); 
    this.stringToHash = new Map(); // Nueva: Para saber que "Movie" -> 74472349
    this.isFitted = false;
  }

  /**
   * Genera el mismo hash que el worker para poder comparar.
   */
  _hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0; 
    }
    return hash;
  }

  fit(df, columns) {
    const cols = Array.isArray(columns) ? columns : [columns];
    
    for (const col of cols) {
      const uniqueValues = new Set();
      const data = df.columns[col];
      
      for (let i = 0; i < df.rowCount; i++) {
        const value = data[i];
        if (value !== null && !isNaN(value) && value !== -1) {
          uniqueValues.add(value);
        }
      }
      
      // Ordenamos los hashes para que el índice sea consistente
      const sortedHashes = Array.from(uniqueValues).sort((a, b) => a - b);
      const colMap = new Map();
      const invColArray = [];

      sortedHashes.forEach((hash, idx) => {
        colMap.set(hash, idx);
        invColArray[idx] = hash; 
      });
      
      this.vocabulary.set(col, colMap);
      this.inverseVocabulary.set(col, invColArray);
    }
    
    this.isFitted = true;
    return this;
  }

  transform(df) {
    if (!this.isFitted) throw new Error('StringIndexer must be fitted first');
    const newColumns = { ...df.columns };
    
    for (const [col, colMap] of this.vocabulary.entries()) {
      const sourceData = df.columns[col];
      const rowCount = df.rowCount;
      const indexedData = new Float64Array(new SharedArrayBuffer(rowCount * 8));
      
      for (let i = 0; i < rowCount; i++) {
        const val = sourceData[i];
        const index = colMap.get(val);
        indexedData[i] = index !== undefined ? index : -1;
      }
      
      newColumns[`${col}_indexed`] = indexedData;
    }
    
    return new DataFrame({ columns: newColumns, rowCount: df.rowCount });
  }

  fitTransform(df, columns) {
    return this.fit(df, columns).transform(df);
  }

  /**
   * IMPORTANTE: Ahora busca el índice basándose en el TEXTO original
   * traduciéndolo primero a Hash.
   */
  getIndex(col, label) {
    const colMap = this.vocabulary.get(col);
    if (!colMap) return -1;
    const hash = this._hashString(label);
    const idx = colMap.get(hash);
    return idx !== undefined ? idx : -1;
  }

  /**
   * Recupera las etiquetas (labels) de una columna.
   * Como no tenemos el texto original en el buffer, 
   * este método ahora es secundario.
   */
  getLabels(col) {
    return this.inverseVocabulary.get(col) || [];
  }
}