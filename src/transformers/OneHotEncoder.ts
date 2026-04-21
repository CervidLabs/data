import type { DataFrame } from '../core/DataFrame.js';

export interface OneHotOptions {
  dropFirst?: boolean;
}

/**
 * OneHotEncoder Nitro - Convierte variables categóricas en columnas binarias (0/1)
 * Optimizado para usar TypedArrays (Uint8Array) para minimizar el impacto en memoria.
 */
type CategoryValue = string | number | boolean;
export class OneHotEncoder {
  private options: Required<OneHotOptions>;
  private categories: Map<string, CategoryValue[]>;
  private isFitted: boolean;

  constructor(options: OneHotOptions = {}) {
    this.options = {
      dropFirst: false,
      ...options,
    };
    this.categories = new Map();
    this.isFitted = false;
  }

  /**
   * Identifica las categorías únicas en la columna seleccionada
   */
  public fit(df: DataFrame, column: string): this {
    const colData = df.columns[column];
    if (!colData) {
      throw new Error(`La columna '${column}' no existe en el DataFrame.`);
    }

    const uniqueIds = new Set<CategoryValue>();

    // Loop de alta velocidad
    for (let i = 0; i < df.rowCount; i++) {
      const id = colData[i];
      // Cast seguro: confiamos en que los datos de la columna
      // son compatibles con CategoryValue tras la validación
      if (id !== undefined && id !== null) {
        uniqueIds.add(id as CategoryValue);
      }
    }

    // Ordenamos para que las columnas generadas tengan un orden predecible
    const sortedCategories = Array.from(uniqueIds).sort((a, b) => {
      if (typeof a === 'number' && typeof b === 'number') {
        return a - b;
      }
      return String(a).localeCompare(String(b));
    });

    this.categories.set(column, sortedCategories);
    this.isFitted = true;
    return this;
  }

  /**
   * Transforma una columna categórica en múltiples columnas binarias.
   * Devuelve un objeto con las nuevas columnas listas para integrar al DataFrame.
   */
  public transform(df: DataFrame, column: string): Record<string, Uint8Array> {
    if (!this.isFitted || !this.categories.has(column)) {
      this.fit(df, column);
    }

    let categories = this.categories.get(column)!;
    const colData = df.columns[column];
    const rowCount = df.rowCount;

    // Lógica para evitar la trampa de la multicolinealidad en ML
    if (this.options.dropFirst) {
      categories = categories.slice(1);
    }

    // Pre-asignamos memoria contigua (Uint8Array es perfecto: solo 1 byte por celda)
    const resultCols: Record<string, Uint8Array> = {};
    const categoryArrays: Uint8Array[] = [];

    categories.forEach((cat) => {
      const colName = `${column}_${cat}`;
      const arr = new Uint8Array(rowCount);
      resultCols[colName] = arr;
      categoryArrays.push(arr);
    });

    // Loop principal: O(N * K) donde N=filas y K=categorías
    for (let i = 0; i < rowCount; i++) {
      const currentId = colData[i];

      for (let j = 0; j < categories.length; j++) {
        if (currentId === categories[j]) {
          categoryArrays[j][i] = 1;
          break; // Una vez encontrado el 1, saltamos a la siguiente fila
        }
      }
    }

    return resultCols;
  }

  /**
   * Ajusta y transforma, devolviendo las columnas binarias
   */
  public fitTransform(df: DataFrame, column: string): Record<string, Uint8Array> {
    return this.transform(df, column);
  }
}
