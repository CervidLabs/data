import { DataFrame, type ColumnData } from '../core/DataFrame.js';

/**
 * StandardScaler - Estandarización de características eliminando la media
 * y escalando a la varianza unitaria (Z-score).
 */
export class StandardScaler {
  private mean_: Map<string, number>;
  private std_: Map<string, number>;
  private isFitted: boolean;

  constructor() {
    this.mean_ = new Map();
    this.std_ = new Map();
    this.isFitted = false;
  }

  /**
   * Calcula la media y la desviación estándar para el entrenamiento.
   */
  public fit(df: DataFrame, columns: string | string[]): this {
    const cols = Array.isArray(columns) ? columns : [columns];

    for (const col of cols) {
      const columnData = df.columns[col];
      if (!columnData) {
        continue;
      }

      // Convertimos a array y filtramos números válidos
      const values = Array.from(columnData).filter((v): v is number => typeof v === 'number' && !isNaN(v));

      if (values.length === 0) {
        continue;
      }

      // Cálculo de Media (µ)
      const mean = values.reduce((a, b) => a + b, 0) / values.length;

      // Cálculo de Varianza (σ²) -> Desviación Estándar (σ)
      const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
      const std = Math.sqrt(variance);

      this.mean_.set(col, mean);
      // Evitamos división por cero si todos los valores son iguales
      this.std_.set(col, std === 0 ? 1 : std);
    }

    this.isFitted = true;
    return this;
  }

  /**
   * Aplica la estandarización: z = (x - u) / s
   */
  public transform(df: DataFrame): DataFrame {
    if (!this.isFitted) {
      throw new Error('StandardScaler militar bloqueado: Debe ejecutar .fit() antes de transformar.');
    }

    // Clonamos la estructura de columnas actual
    const newColumns: Record<string, ColumnData> = { ...df.columns };

    for (const [col, mean] of this.mean_.entries()) {
      const std = this.std_.get(col) ?? 1;
      const scaledColName = `${col}_scaled`;

      // Buffer temporal para permitir .push() y evitar errores de TypedArray
      // number | string | null se maneja como unknown para permitir ambos casos
      const tempArray: unknown[] = [];
      for (let i = 0; i < df.rowCount; i++) {
        const value = df.columns[col][i];

        if (typeof value === 'number' && !isNaN(value)) {
          // Fórmula Z-score
          tempArray.push((value - mean) / std);
        } else {
          tempArray.push(value); // Preservar no-numéricos
        }
      }

      // Inyectamos como ColumnData (JS Array o TypedArray)
      newColumns[scaledColName] = tempArray;
    }

    return new DataFrame({
      columns: newColumns,
      rowCount: df.rowCount,
    });
  }

  /**
   * Ajusta y transforma en un solo paso.
   */
  public fitTransform(df: DataFrame, columns: string | string[]): DataFrame {
    return this.fit(df, columns).transform(df);
  }
}
