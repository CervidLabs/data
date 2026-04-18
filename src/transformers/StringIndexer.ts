import { DataFrame } from '../core/DataFrame.js';

export interface StringIndexerOptions {
  handleUnknown?: 'keep' | 'error';
}

/**
 * StringIndexer - Codifica columnas de texto a índices numéricos
 * utilizando acceso directo a los offsets del buffer original.
 */
export class StringIndexer {
  private options: Required<StringIndexerOptions>;
  private maps: Record<string, Map<string, number>>;
  private labels: Record<string, string[]>;
  private decoder: InstanceType<typeof TextDecoder>;
  private isFitted: boolean;

  constructor(options: StringIndexerOptions = {}) {
    this.options = {
      handleUnknown: 'keep',
      ...options,
    };
    this.maps = {};
    this.labels = {};
    this.decoder = new TextDecoder();
    this.isFitted = false;
  }

  /**
   * Entrena el indexador y transforma la columna en un solo paso.
   */
  public fitTransform(df: DataFrame, input: string | string[]): Float64Array {
    // Normalizar nombre de columna (tomamos la primera si es un array)
    const colName = Array.isArray(input) ? input[0] : input;

    const distinctValues = new Set<string>();
    const map = new Map<string, number>();
    const labels: string[] = [];

    // 1. Escaneo de bytes usando los offsets del DF (Zero-copy extraction)
    for (let i = 0; i < df.rowCount; i++) {
      const val = this._extract(df, colName, i);
      distinctValues.add(val);
    }

    // 2. Crear Diccionario ordenado
    Array.from(distinctValues)
      .sort()
      .forEach((val, idx) => {
        map.set(val, idx);
        labels.push(val);
      });

    // 3. Guardar metadatos bajo el nombre de la columna
    this.maps[colName] = map;
    this.labels[colName] = labels;
    this.isFitted = true;

    // 4. Transformar a Float64Array para compatibilidad con el motor Nitro
    const result = new Float64Array(df.rowCount);
    for (let i = 0; i < df.rowCount; i++) {
      const val = this._extract(df, colName, i);
      // Si no existe, usamos el índice N (handleUnknown: keep)
      result[i] = map.get(val) ?? labels.length;
    }

    return result;
  }

  /**
   * Extrae el string directamente del buffer original sin crear objetos intermedios pesados.
   */
  private _extract(df: DataFrame, colName: string, rowIdx: number): string {
    if (!df.colMap) throw new Error('DataFrame no tiene colMap.');
    const colIdx = df.colMap[colName];
    if (colIdx === undefined) throw new Error(`Columna "${colName}" no encontrada.`);

    // Verificación de grado militar para evitar errores de memoria
    if (!df.offsets) throw new Error('DataFrame no tiene offsets cargados.');

    // Cálculo del puntero en el buffer de offsets
    // Cada celda tiene 2 valores (start, end)
    const offIdx = (rowIdx * Object.keys(df.colMap).length + colIdx) * 2;
    const start = df.offsets[offIdx];
    const end = df.offsets[offIdx + 1];

    if (!df.originalBuffer) throw new Error('El buffer original es inaccesible.');

    return this.decoder.decode(new Uint8Array(df.originalBuffer, start, end - start)).trim();
  }

  /**
   * Devuelve la lista de etiquetas originales para una columna.
   */
  public getLabels(colName?: string): string[] {
    const target = colName || Object.keys(this.labels)[0];
    if (!target || !this.labels[target]) {
      throw new Error(`StringIndexer: No hay etiquetas disponibles para "${target}"`);
    }
    return this.labels[target];
  }

  /**
   * Busca el índice numérico de una etiqueta específica.
   */
  public getIndex(colName: string, label?: string): number {
    // Sobrecarga: si solo viene un argumento, lo tratamos como el label de la primera columna
    if (label === undefined) {
      const firstCol = Object.keys(this.maps)[0];
      if (!firstCol) return -1;
      return this.maps[firstCol].get(colName) ?? -1;
    }

    const map = this.maps[colName];
    return map ? (map.get(label) ?? -1) : -1;
  }
}
