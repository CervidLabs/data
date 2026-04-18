// 1. Definimos los tipos de memoria soportados para esta Serie
export type SeriesDataType = 'float64' | 'int32';
export type SeriesDataArray = Float64Array | Int32Array;

// 2. Contrato estricto para el "Dictionary Encoder" (StringIndexer)
export interface IStringIndexer {
  getStringById(id: number): string | null | undefined;
}

// 3. Interfaz de retorno para los resultados de agregación
export interface AggregationResult {
  data: Record<string | number, number>;
  show: (n?: number) => void;
}

/**
 * Clase Series: El pilar columnar de Cervid.
 * Optimizada para no usar objetos Map y trabajar sobre memoria plana.
 */
export class Series {
  public name: string;
  public data: SeriesDataArray;
  public type: SeriesDataType;
  public indexer: IStringIndexer | null;
  public mask: Uint8Array | null;

  constructor(name: string, data: SeriesDataArray, type: SeriesDataType, indexer: IStringIndexer | null = null, mask: Uint8Array | null = null) {
    this.name = name;
    this.data = data;
    this.type = type;
    this.indexer = indexer;
    this.mask = mask;
  }

  /**
   * Obtiene el valor en un índice.
   * Si tiene un indexer, traduce el ID numérico a String (ASIN) al vuelo.
   */
  get(index: number): number | string {
    const val = this.data[index];

    if (this.indexer && typeof this.indexer.getStringById === 'function') {
      const strVal = this.indexer.getStringById(val);
      // Si el diccionario no encuentra el valor, devolvemos el ID original
      return strVal !== undefined && strVal !== null ? strVal : val;
    }

    return val;
  }

  /**
   * Devuelve la longitud de la serie
   */
  get length(): number {
    return this.data.length;
  }

  /**
   * Crea una porción de la serie (usado por .limit())
   * Mantiene la referencia al indexer para que la traducción siga funcionando.
   */
  slice(start: number, end?: number): Series {
    // TypedArrays tienen el método slice nativo, pero TS necesita que le
    // aseguremos que el resultado no pierde su tipo original.
    const slicedData = this.data.slice(start, end) as SeriesDataArray;

    return new Series(this.name, slicedData, this.type, this.indexer, this.mask);
  }

  /**
   * Método estático para reconstruir Series desde buffers crudos
   */
  static fromRawBuffer(
    name: string,
    data: SeriesDataArray,
    type: SeriesDataType,
    indexer: IStringIndexer | null = null,
    mask: Uint8Array | null = null,
  ): Series {
    return new Series(name, data, type, indexer, mask);
  }
}
