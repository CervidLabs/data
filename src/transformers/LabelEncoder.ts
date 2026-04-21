/**
 * LabelEncoder - Codifica etiquetas con valores entre 0 y n_clases-1.
 * @template T El tipo de las etiquetas originales (string, number, etc.)
 */
export class LabelEncoder<T = string | number> {
  private classes_: T[];
  private classToIndex: Map<T, number>;
  private isFitted: boolean;

  constructor() {
    this.classes_ = [];
    this.classToIndex = new Map<T, number>();
    this.isFitted = false;
  }

  /**
   * Ajusta el codificador con los valores proporcionados.
   */
  public fit(values: T[]): this {
    // Filtramos valores nulos/indefinidos y obtenemos valores únicos
    const uniqueValues = Array.from(new Set(values.filter((v) => v !== null && v !== undefined)));

    // Ordenamos para mantener consistencia en la codificación
    this.classes_ = uniqueValues.sort((a, b) => {
      if (a < b) {
        return -1;
      }
      if (a > b) {
        return 1;
      }
      return 0;
    });

    this.classToIndex.clear();
    this.classes_.forEach((cls, idx) => {
      this.classToIndex.set(cls, idx);
    });

    this.isFitted = true;
    return this;
  }

  /**
   * Transforma etiquetas en índices numéricos.
   */
  public transform(values: T[]): number[] {
    if (!this.isFitted) {
      throw new Error('LabelEncoder must be fitted before calling transform()');
    }

    return values.map((v) => {
      if (v === null || v === undefined) {
        return -1;
      }
      // Si el valor no existe en el entrenamiento, devolvemos -1
      return this.classToIndex.get(v) ?? -1;
    });
  }

  /**
   * Ajusta y transforma en un solo paso.
   */
  public fitTransform(values: T[]): number[] {
    return this.fit(values).transform(values);
  }

  /**
   * Revierte los índices a sus etiquetas originales.
   */
  public inverseTransform(indices: number[]): (T | null)[] {
    if (!this.isFitted) {
      throw new Error('LabelEncoder must be fitted before calling inverseTransform()');
    }

    return indices.map((idx) => {
      if (idx < 0 || idx >= this.classes_.length) {
        return null;
      }
      return this.classes_[idx];
    });
  }

  /**
   * Devuelve las clases aprendidas por el codificador.
   */
  public getClasses(): T[] {
    return [...this.classes_];
  }
}
