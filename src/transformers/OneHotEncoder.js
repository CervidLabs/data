import { DataFrame } from '../core/DataFrame.js';
/**
 * OneHotEncoder - Convierte categorías a columnas binarias
 */
export class OneHotEncoder {
  constructor(options = {}) {
    this.options = {
      dropFirst: false,     // Eliminar primera columna para evitar multicolinealidad
      sparse: false,        // Usar representación sparse
      handleUnknown: 'ignore',
      ...options
    };
    this.categories = new Map();
    this.featureNames = [];
    this.isFitted = false;
  }

  fit(df, columns) {
    const cols = Array.isArray(columns) ? columns : [columns];
    
    for (const col of cols) {
      // Obtener valores únicos no nulos
      const uniqueValues = new Set();
      for (let i = 0; i < df.rowCount; i++) {
        const value = df.columns[col]?.[i];
        if (value !== null && value !== undefined && value !== '') {
          // Para Netflix, a veces el valor es un string con múltiples países
          if (typeof value === 'string' && value.includes(',')) {
            // Si contiene comas, podría ser múltiples valores
            const parts = value.split(',').map(v => v.trim());
            for (const part of parts) {
              if (part) uniqueValues.add(part);
            }
          } else {
            uniqueValues.add(String(value));
          }
        }
      }
      
      let categories = Array.from(uniqueValues).sort();
      if (this.options.dropFirst && categories.length > 1) {
        categories = categories.slice(1);
      }
      
      this.categories.set(col, categories);
      
      // Generar nombres de columnas seguros
      for (const category of categories) {
        // Sanitizar el nombre de la columna
        let safeName = `${col}_${category}`;
        safeName = safeName
          .replace(/[^a-zA-Z0-9_]/g, '_')  // Reemplazar caracteres especiales
          .replace(/_+/g, '_')              // Múltiples underscores a uno
          .replace(/^_|_$/g, '');           // Remover underscores al inicio/final
        
        this.featureNames.push(safeName);
      }
    }
    
    this.isFitted = true;
    return this;
  }

  transform(df) {
    if (!this.isFitted) {
      throw new Error('OneHotEncoder must be fitted first');
    }
    
    const newColumns = { ...df.columns };
    let featureIdx = 0;
    
    for (const [col, categories] of this.categories.entries()) {
      for (const category of categories) {
        const safeName = this.featureNames[featureIdx];
        newColumns[safeName] = [];
        
        for (let i = 0; i < df.rowCount; i++) {
          const value = df.columns[col]?.[i];
          let isMatch = false;
          
          if (value !== null && value !== undefined && value !== '') {
            const strValue = String(value);
            // Verificar si coincide o si contiene la categoría (para múltiples valores)
            if (strValue === category) {
              isMatch = true;
            } else if (strValue.includes(',') && category !== '') {
              const parts = strValue.split(',').map(v => v.trim());
              isMatch = parts.includes(category);
            }
          }
          
          newColumns[safeName].push(isMatch ? 1 : 0);
        }
        
        featureIdx++;
      }
    }
    
    return new DataFrame({ 
      columns: newColumns, 
      rowCount: df.rowCount 
    });
  }

  fitTransform(df, columns) {
    return this.fit(df, columns).transform(df);
  }

  getFeatureNames() {
    return this.featureNames;
  }
}