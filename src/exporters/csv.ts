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

  async export(outputPath: string): Promise<ExportResult> {
    const stream = fs.createWriteStream(outputPath, {
      encoding: this.options.encoding,
    });

    // Usamos headers en lugar de Object.keys para garantizar el orden exacto
    const columns = this.df.headers;

    // Escribir header
    if (this.options.header) {
      stream.write(columns.join(this.options.delimiter) + '\n');
    }

    // Escribir filas
    for (let i = 0; i < this.df.rowCount; i++) {
      const row = columns.map((col) => {
        // Le confirmamos a TS que vamos a leer la memoria como un arreglo indexable
        const colData = this.df.columns[col] as ArrayLike<unknown>;
        const value = colData[i];

        if (value === null || value === undefined) {
          return '';
        }

        // Forzamos la conversión a String para poder usar .includes() de forma segura
        let strValue = String(value as string);

        // Agregamos '\n' a la regla de escape (Estándar RFC 4180 de CSV)
        if (strValue.includes(this.options.delimiter) || strValue.includes('"') || strValue.includes('\n')) {
          strValue = `"${strValue.replace(/"/g, '""')}"`;
        }

        return strValue;
      });

      stream.write(row.join(this.options.delimiter) + '\n');

      // Liberar memoria (Dejar respirar al Event Loop de Node) cada 100k filas
      if (i % 100000 === 0 && i > 0) {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    }

    // CRÍTICO: En Vanilla JS hacías stream.end() y retornabas el objeto inmediatamente.
    // Esto podía causar corrupción de datos si Node.js no había terminado de flushear el buffer al disco.
    // Ahora lo envolvemos en una Promesa estricta.
    return new Promise((resolve, reject) => {
      stream.on('finish', () => {
        console.info(`✅ CSV exportado: ${outputPath} (${this.df.rowCount.toLocaleString()} filas)`);
        resolve({ path: outputPath, rows: this.df.rowCount });
      });

      stream.on('error', (err) => {
        reject(err);
      });

      stream.end();
    });
  }
}
