import fs from 'fs';
import type { DataFrame } from '../core/DataFrame.js';

// 1. Definimos la interfaz estricta de configuración
export interface CSVExporterOptions {
  delimiter?: string;
  header?: boolean;
  encoding?: BufferEncoding;
}

// 2. Definimos el contrato de lo que devuelve la función
export interface ExportResult {
  path: string;
  rows: number;
}

export class CSVExporter {
  private df: DataFrame;
  // Usamos Required<> para decirle al compilador que, internamente,
  // estas opciones siempre tendrán un valor (por los valores por defecto)
  private options: Required<CSVExporterOptions>;

  constructor(df: DataFrame, options: CSVExporterOptions = {}) {
    this.df = df;
    this.options = {
      delimiter: ',',
      header: true,
      encoding: 'utf8',
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
      return JSON.stringify(value) ?? '';
    } catch {
      return '';
    }
  }

  async export(outputPath: string): Promise<ExportResult> {
    const stream = fs.createWriteStream(outputPath, {
      encoding: this.options.encoding,
    });

    const columns = this.df.headers;

    if (this.options.header) {
      stream.write(columns.join(this.options.delimiter) + '\n');
    }

    for (let i = 0; i < this.df.rowCount; i++) {
      const row = columns.map((col) => {
        const colData = this.df.columns[col] as ArrayLike<unknown>;
        const value = colData[i];

        let strValue = this.formatValue(value);

        if (strValue.includes(this.options.delimiter) || strValue.includes('"') || strValue.includes('\n')) {
          strValue = `"${strValue.replace(/"/g, '""')}"`;
        }

        return strValue;
      });

      stream.write(row.join(this.options.delimiter) + '\n');

      if (i % 100000 === 0 && i > 0) {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    }

    return new Promise<ExportResult>((resolve, reject) => {
      stream.on('finish', () => {
        console.info(`✅ CSV exportado: ${outputPath} (${this.df.rowCount.toLocaleString()} filas)`);

        resolve({
          path: outputPath,
          rows: this.df.rowCount,
        });
      });

      stream.on('error', (err) => {
        reject(err);
      });

      stream.end();
    });
  }
}
