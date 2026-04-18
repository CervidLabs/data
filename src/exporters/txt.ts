import fs from 'fs';
import { DataFrame } from '../core/DataFrame.js';

// 1. Interfaces estrictas
export interface TXTExporterOptions {
  delimiter?: string;
  header?: boolean;
  maxRows?: number;
  encoding?: BufferEncoding;
}

export interface TXTExportResult {
  path: string;
  rows: number;
  totalRows: number;
}

export class TXTExporter {
  private df: DataFrame;
  private options: Required<TXTExporterOptions>;

  constructor(df: DataFrame, options: TXTExporterOptions = {}) {
    this.df = df;
    this.options = {
      delimiter: ' | ',
      header: true,
      maxRows: 1000,
      encoding: 'utf8', // Añadido para mantener consistencia con los demás
      ...options,
    };
  }

  async export(outputPath: string): Promise<TXTExportResult> {
    const columns = this.df.headers; // Usamos headers por seguridad de orden
    const maxRows = Math.min(this.options.maxRows, this.df.rowCount);
    const lines: string[] = [];

    // Calcular anchos de columna
    const colWidths: Record<string, number> = {};

    columns.forEach((col) => {
      let maxLen = col.length;
      const colData = this.df.columns[col] as ArrayLike<unknown>;

      for (let i = 0; i < maxRows; i++) {
        const rawVal = colData[i];
        // Parseo seguro sin asumir el operador ?? sobre null/undefined de forma opaca
        const strVal = rawVal === null || rawVal === undefined ? '' : String(rawVal);
        maxLen = Math.max(maxLen, strVal.length);
      }
      colWidths[col] = Math.min(maxLen, 50); // Límite duro de 50 caracteres
    });

    // Construir Header
    if (this.options.header) {
      const headerLine = columns.map((col) => col.padEnd(colWidths[col])).join(this.options.delimiter);

      lines.push(headerLine);
      lines.push('-'.repeat(headerLine.length));
    }

    // Construir Filas
    for (let i = 0; i < maxRows; i++) {
      const row = columns.map((col) => {
        const rawVal = (this.df.columns[col] as ArrayLike<unknown>)[i];
        let val = rawVal === null || rawVal === undefined ? '' : String(rawVal);

        if (val.length > 50) {
          val = val.slice(0, 47) + '...';
        }

        return val.padEnd(colWidths[col]);
      });

      lines.push(row.join(this.options.delimiter));
    }

    // Pie de página si hay datos truncados
    if (this.df.rowCount > maxRows) {
      lines.push(`\n... y ${(this.df.rowCount - maxRows).toLocaleString()} filas más`);
    }

    // Escribimos todo de golpe. Aquí no hay riesgo de "Bomba de Memoria"
    // porque maxRows siempre limita el tamaño del arreglo 'lines'.
    await fs.promises.writeFile(outputPath, lines.join('\n'), this.options.encoding);

    console.info(`✅ TXT exportado: ${outputPath} (${maxRows.toLocaleString()} filas mostradas)`);

    return {
      path: outputPath,
      rows: maxRows,
      totalRows: this.df.rowCount,
    };
  }
}
