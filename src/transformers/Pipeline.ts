import { type ColumnData, DataFrame } from '../core/DataFrame.js';

/**
 * Interface militar para cualquier transformador de Cervid
 */
export type TransformResult = DataFrame | Record<string, ColumnData> | Uint8Array | Float64Array;

export interface Transformer {
  fit?(df: DataFrame, columns?: string | string[]): this | Promise<this>;

  // 🛡️ Ya no más 'any'. Ahora es explícito.
  transform(df: DataFrame, columns?: string | string[]): TransformResult;

  fitTransform?(df: DataFrame, columns?: string | string[]): TransformResult;

  columns?: string | string[];
}
/**
 * Pipeline - Encadena múltiples etapas de pre-procesamiento de forma secuencial
 */
export class Pipeline {
  private stages: Transformer[];
  private isFitted: boolean;

  constructor(stages: Transformer[] = []) {
    this.stages = stages;
    this.isFitted = false;
  }

  /**
   * Añade una nueva etapa al final del pipeline
   */
  public add(stage: Transformer): this {
    this.stages.push(stage);
    this.isFitted = false; // Si añadimos una etapa, el pipeline ya no está entrenado
    return this;
  }

  /**
   * Entrena todas las etapas del pipeline secuencialmente
   */
  public async fit(df: DataFrame): Promise<this> {
    let currentDf = df;

    for (const stage of this.stages) {
      if (stage.fit) {
        // Ejecutamos el fit. Si el stage tiene columnas pre-definidas, las usamos.
        await stage.fit(currentDf, stage.columns);
      }

      // Para pipelines complejos, a veces el fit necesita transformar
      // para que la siguiente etapa vea los datos actualizados
      if (stage.transform) {
        const result = stage.transform(currentDf, stage.columns);
        // Si el resultado es un DataFrame (como en scalers), actualizamos la referencia
        if (result instanceof DataFrame) {
          currentDf = result;
        }
      }
    }

    this.isFitted = true;
    return this;
  }

  /**
   * Aplica todas las transformaciones en cadena
   */
  public transform(df: DataFrame): DataFrame {
    if (!this.isFitted) {
      throw new Error('Pipeline militar bloqueado: Debe ejecutar .fit() antes de transformar.');
    }

    let currentDf = df;

    for (const stage of this.stages) {
      const result = stage.transform(currentDf, stage.columns);

      // El Pipeline siempre espera devolver un DataFrame consolidado
      if (result instanceof DataFrame) {
        currentDf = result;
      } else if (typeof result === 'object' && !Array.isArray(result) && !(result instanceof Uint8Array) && !(result instanceof Float64Array)) {
        // Si el transformador devolvió columnas sueltas (como OneHotEncoder), las integramos
        currentDf.assign(result);
      }
    }

    return currentDf;
  }

  /**
   * Entrena y transforma en una sola operación
   */
  public async fitTransform(df: DataFrame): Promise<DataFrame> {
    await this.fit(df);
    return this.transform(df);
  }
}
