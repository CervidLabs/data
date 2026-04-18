import fs from 'fs';
import { DataFrame } from '../core/DataFrame.js';

// 1. Interfaces estrictas de configuración y retorno
export interface JSONExporterOptions {
  pretty?: boolean;
  ndjson?: boolean; // Newline Delimited JSON
  encoding?: BufferEncoding;
}

export interface JSONExportResult {
  path: string;
  rows: number;
  format: 'json' | 'ndjson';
}

export class JSONExporter {
  private df: DataFrame;
  private options: Required<JSONExporterOptions>;

  constructor(df: DataFrame, options: JSONExporterOptions = {}) {
    this.df = df;
    this.options = {
      pretty: false,
      ndjson: false,
      encoding: 'utf8',
      ...options,
    };
  }

  async export(outputPath: string): Promise<JSONExportResult> {
    if (this.options.ndjson) {
      return await this._exportNDJSON(outputPath);
    }
    return await this._exportJSON(outputPath);
  }

  private async _exportNDJSON(outputPath: string): Promise<JSONExportResult> {
    const stream = fs.createWriteStream(outputPath, { encoding: this.options.encoding });
    const columns = this.df.headers;

    for (let i = 0; i < this.df.rowCount; i++) {
      const row: Record<string, unknown> = {};

      columns.forEach((col) => {
        row[col] = (this.df.columns[col] as ArrayLike<unknown>)[i];
      });

      stream.write(JSON.stringify(row) + '\n');

      if (i % 100000 === 0 && i > 0) {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    }

    // CRÍTICO: Reparado el bug de concurrencia. Ahora esperamos a que el disco termine.
    return new Promise((resolve, reject) => {
      stream.on('finish', () => {
        console.info(`✅ NDJSON exportado: ${outputPath} (${this.df.rowCount.toLocaleString()} filas)`);
        resolve({ path: outputPath, rows: this.df.rowCount, format: 'ndjson' });
      });

      stream.on('error', (err) => reject(err));

      stream.end();
    });
  }

  private async _exportJSON(outputPath: string): Promise<JSONExportResult> {
    // ADVERTENCIA ARQUITECTÓNICA: Bomba de Memoria potencial.
    const data = this.df.toArray();
    const json = this.options.pretty ? JSON.stringify(data, null, 2) : JSON.stringify(data);

    await fs.promises.writeFile(outputPath, json, this.options.encoding);

    console.info(`✅ JSON exportado: ${outputPath} (${this.df.rowCount.toLocaleString()} filas)`);

    return { path: outputPath, rows: this.df.rowCount, format: 'json' };
  }
}
