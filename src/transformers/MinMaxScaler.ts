import { DataFrame, ColumnData } from '../core/DataFrame.js';

export interface MinMaxScalerOptions {
  featureRange?: [number, number];
}

/**
 * MinMaxScaler - Normalización lineal a un rango específico (por defecto [0, 1])
 */
export class MinMaxScaler {
  private options: Required<MinMaxScalerOptions>;
  private min_: Map<string, number>;
  private max_Diff: Map<string, number>; // Guardamos la diferencia (max - min)
  private isFitted: boolean;

  constructor(options: MinMaxScalerOptions = {}) {
    this.options = {
      featureRange: [0, 1],
      ...options,
    };
    this.min_ = new Map();
    this.max_Diff = new Map();
    this.isFitted = false;
  }

  /**
   * Calcula el mínimo y máximo de las columnas seleccionadas
   */
  public fit(df: DataFrame, columns: string | string[]): this {
    const cols = Array.isArray(columns) ? columns : [columns];

    for (const col of cols) {
      const columnData = df.columns[col];
      if (!columnData) continue;

      // Filtramos valores numéricos válidos
      const values = (Array.from(columnData) as unknown[]).filter((v): v is number => typeof v === 'number' && !isNaN(v));

      if (values.length === 0) continue;

      const min = Math.min(...values);
      const max = Math.max(...values);
      const diff = max === min ? 1 : max - min;

      this.min_.set(col, min);
      this.max_Diff.set(col, diff);
    }

    this.isFitted = true;
    return this;
  }

  /**
   * Aplica la transformación y devuelve un nuevo DataFrame con las columnas normalizadas
   */
  public transform(df: DataFrame): DataFrame {
    if (!this.isFitted) {
      throw new Error('MinMaxScaler must be fitted first');
    }

    const [rangeMin, rangeMax] = this.options.featureRange;
    const newColumns: Record<string, ColumnData> = { ...df.columns };

    for (const [col, min] of this.min_.entries()) {
      const diff = this.max_Diff.get(col) || 1;
      const scaledColName = `${col}_normalized`;

      // Buffer temporal para evitar el error de .push() en TypedArrays
      const tempArray: (number | unknown)[] = [];

      for (let i = 0; i < df.rowCount; i++) {
        const value = df.columns[col][i];

        if (typeof value === 'number' && !isNaN(value)) {
          // Fórmula: X_scaled = (X - X_min) / (X_max - X_min)
          const normalized = (value - min) / diff;
          // Ajuste al rango de la opción (featureRange)
          const scaled = normalized * (rangeMax - rangeMin) + rangeMin;
          tempArray.push(scaled);
        } else {
          tempArray.push(value); // Mantener nulls o strings si los hay
        }
      }

      newColumns[scaledColName] = tempArray as ColumnData;
    }

    return new DataFrame({
      columns: newColumns,
      rowCount: df.rowCount,
    });
  }

  /**
   * Ajusta y transforma en un solo paso
   */
  public fitTransform(df: DataFrame, columns: string | string[]): DataFrame {
    return this.fit(df, columns).transform(df);
  }
}
