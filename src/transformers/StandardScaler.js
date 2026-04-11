import { DataFrame } from '../core/DataFrame.js';

/**
 * StandardScaler - Estandarización (media=0, std=1)
 */
export class StandardScaler {
  constructor() {
    this.mean_ = new Map();
    this.std_ = new Map();
    this.isFitted = false;
  }

  fit(df, columns) {
    const cols = Array.isArray(columns) ? columns : [columns];
    
    for (const col of cols) {
      const values = df.columns[col].filter(v => typeof v === 'number' && !isNaN(v));
      const mean = values.reduce((a, b) => a + b, 0) / values.length;
      const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
      const std = Math.sqrt(variance);
      
      this.mean_.set(col, mean);
      this.std_.set(col, std === 0 ? 1 : std);
    }
    
    this.isFitted = true;
    return this;
  }

  transform(df) {
    if (!this.isFitted) {
      throw new Error('StandardScaler must be fitted first');
    }
    
    const newColumns = { ...df.columns };
    
    for (const [col, mean] of this.mean_.entries()) {
      const std = this.std_.get(col);
      const scaledCol = `${col}_scaled`;
      
      newColumns[scaledCol] = [];
      for (let i = 0; i < df.rowCount; i++) {
        const value = df.columns[col][i];
        if (typeof value === 'number' && !isNaN(value)) {
          newColumns[scaledCol].push((value - mean) / std);
        } else {
          newColumns[scaledCol].push(value);
        }
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
}