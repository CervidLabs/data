import fs from 'fs';
import type { DataFrame } from '../core/DataFrame.js';

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
  private formatValue(value: unknown): string {
    if (value === null || value === undefined) {
      return '';
    }

    if (typeof value === 'string') {
      return value;
    }

    if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
      return value.toString();
    }

    try {
      const json = JSON.stringify(value);
      return json ?? '';
    } catch {
      return '';
    }
  }

  async export(outputPath: string): Promise<TXTExportResult> {
    const columns = this.df.headers;
    const maxRows = Math.min(this.options.maxRows, this.df.rowCount);
    const lines: string[] = [];

    const colWidths: Record<string, number> = {};

    columns.forEach((col) => {
      let maxLen = col.length;
      const colData = this.df.columns[col] as ArrayLike<unknown>;

      for (let i = 0; i < maxRows; i++) {
        const rawVal = colData[i];
        const strVal = this.formatValue(rawVal);

        maxLen = Math.max(maxLen, strVal.length);
      }

      colWidths[col] = Math.min(maxLen, 50);
    });

    if (this.options.header) {
      const headerLine = columns.map((col) => col.padEnd(colWidths[col] ?? col.length)).join(this.options.delimiter);

      lines.push(headerLine);
      lines.push('-'.repeat(headerLine.length));
    }

    for (let i = 0; i < maxRows; i++) {
      const row = columns.map((col) => {
        const colData = this.df.columns[col] as ArrayLike<unknown>;
        const rawVal = colData[i];

        let val = this.formatValue(rawVal);

        if (val.length > 50) {
          val = `${val.slice(0, 47)}...`;
        }

        return val.padEnd(colWidths[col] ?? col.length);
      });

      lines.push(row.join(this.options.delimiter));
    }

    if (this.df.rowCount > maxRows) {
      lines.push(`\n... y ${(this.df.rowCount - maxRows).toLocaleString()} filas más`);
    }

    await fs.promises.writeFile(outputPath, lines.join('\n'), this.options.encoding);

    return {
      path: outputPath,
      rows: maxRows,
      totalRows: this.df.rowCount,
    };
  }
}
