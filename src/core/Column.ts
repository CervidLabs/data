import type { DataFrame, ColumnData } from './DataFrame.js';

export class Column {
  public name: string;
  public data: ColumnData;
  public length: number;
  public parentDf: DataFrame;

  constructor(name: string, data: ColumnData, parentDf: DataFrame) {
    this.name = name;
    this.data = data;
    // Tanto TypedArrays como Arrays normales tienen la propiedad length
    this.length = data.length;
    this.parentDf = parentDf;
  }

  add(value: number | Column): this {
    const isCol = value instanceof Column;
    // Casteo duro: Le garantizamos a TS que trataremos esto como un array numérico
    const tData = this.data as Float64Array | number[];
    const vData = isCol ? (value.data as Float64Array | number[]) : null;
    const scalar = !isCol ? value : 0;

    for (let i = 0; i < this.length; i++) {
      tData[i] += isCol && vData ? vData[i] : scalar;
    }
    return this;
  }

  sub(value: number | Column): this {
    const isCol = value instanceof Column;
    const tData = this.data as Float64Array | number[];
    const vData = isCol ? (value.data as Float64Array | number[]) : null;
    const scalar = !isCol ? value : 0;

    for (let i = 0; i < this.length; i++) {
      tData[i] -= isCol && vData ? vData[i] : scalar;
    }
    return this;
  }

  mul(value: number | Column): this {
    const isCol = value instanceof Column;
    const tData = this.data as Float64Array | number[];
    const vData = isCol ? (value.data as Float64Array | number[]) : null;
    const scalar = !isCol ? value : 1; // Precaución: scalar por defecto a 1 si no es columna

    for (let i = 0; i < this.length; i++) {
      tData[i] *= isCol && vData ? vData[i] : scalar;
    }
    return this;
  }

  div(value: number | Column): this {
    const isCol = value instanceof Column;
    const tData = this.data as Float64Array | number[];
    const vData = isCol ? (value.data as Float64Array | number[]) : null;
    const scalar = !isCol ? value : 1;

    for (let i = 0; i < this.length; i++) {
      const divisor = isCol && vData ? vData[i] : scalar;
      // Evitamos división por cero según tu lógica original
      tData[i] /= divisor !== 0 ? divisor : 1;
    }
    return this;
  }

  to_datetime(): this {
    // Aquí el dato original puede ser un string ("2026-01-01")
    // Usamos 'any[]' temporalmente para permitir mutar de String a Number en el mismo espacio
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const tData = this.data as any[];

    for (let i = 0; i < this.length; i++) {
      tData[i] = new Date(tData[i]).getTime();
    }
    return this;
  }

  /**
   * Extrae la hora (0-23) de un timestamp milisegundos.
   * Crea una nueva columna en el DataFrame padre.
   */
  extract_hour(offsetSeconds = 32400): this {
    const hours = new Float64Array(this.length);
    const tData = this.data as Float64Array | number[];

    for (let i = 0; i < this.length; i++) {
      // Convertimos ms a segundos, restamos offset, volvemos a horas
      const totalSeconds = Math.floor(tData[i] / 1000) - offsetSeconds;
      const secondsInDay = ((totalSeconds % 86400) + 86400) % 86400;
      hours[i] = Math.floor(secondsInDay / 3600);
    }

    const newName = `${this.name}_hour`;
    this.parentDf.columns[newName] = hours;

    if (!this.parentDf?.headers.includes(newName)) {
      this.parentDf.headers.push(newName);
    }

    return this;
  }
}
