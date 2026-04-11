import { DataFrame } from './DataFrame.js';
import { Reader } from './Reader.js';
import { ParallelExecutor } from '../workers/parallel.js'; // Ajusta la ruta

export class Octopus {
  constructor(options = {}) {
    this.options = options;
  }

// src/core/Octopus.js
static async read(path, options = {}) {
    const reader = new Reader(path, options);
    const meta = await reader.parse(); 

    const executor = new ParallelExecutor(path, { 
        headers: meta.headers,
        numWorkers: options.workers || 8 
    });

    // executeIngest devuelve { columns, rowCount }
    const result = await executor.executeIngest(meta); 

    // IMPORTANTE: Verifica que result tenga rowCount antes de enviarlo
    if (!result.rowCount) {
        console.error("⚠️ Error: El executor no devolvió rowCount.");
    }

    return new DataFrame(result); 
}
  /**
   * Lee un archivo JSON específicamente
   * @param {string} path 
   * @returns {Promise<DataFrame>}
   */
  static async readJSON(path, options = {}) {
    return await this.read(path, { ...options, type: 'json' });
  }

  /**
   * Lee un archivo CSV específicamente
   * @param {string} path 
   * @returns {Promise<DataFrame>}
   */
  static async readCSV(path, options = {}) {
    return await this.read(path, { ...options, type: 'csv' });
  }

  /**
   * Crea un DataFrame desde un array
   * @param {Array} data 
   * @returns {DataFrame}
   */
  static fromArray(data) {
    return new DataFrame(data);
  }

  /**
   * Versión lazy (no ejecuta hasta .collect())
   * @param {string} path 
   * @returns {DataFrame}
   */
  static lazy(path, options = {}) {
    return this.read(path, { ...options, lazy: true });
  }
}