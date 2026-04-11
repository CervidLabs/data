import fs from 'fs';
import path from 'path';

export class Reader {
  constructor(filePath, options = {}) {
    this.path = filePath;
    this.options = options;
    this.type = options.type || filePath.split('.').pop();
  }

  /**
   * MÉTODO PRINCIPAL: Carga el archivo a SharedArrayBuffer 
   * y devuelve la metadata para el ParallelExecutor.
   */
  async parse() {
    const stats = fs.statSync(this.path);
    const fd = fs.openSync(this.path, 'r');
    
    // 1. Pre-asignación de Memoria Compartida (Zero-copy target)
    const sharedBuffer = new SharedArrayBuffer(stats.size);
    const view = new Uint8Array(sharedBuffer);
    
    // 2. Ingesta síncrona: M.2 -> RAM sin escalas
    fs.readSync(fd, view, 0, stats.size, 0);
    fs.closeSync(fd);

    if (this.type === 'csv') {
      return this._prepareCSV(view, stats.size);
    } else if (this.type === 'json' || this.type === 'ndjson') {
      return this._prepareJSON(view, stats.size);
    } else {
      throw new Error(`Format ${this.type} not supported by Octopus Nitro.`);
    }
  }

  /**
   * Extrae headers y estima filas para CSV sin crear strings pesados.
   */
  _prepareCSV(view, size) {
    const delimiter = this.options.delimiter || ',';
    
    // Encontrar fin de la primera línea para headers
    let firstNL = 0;
    while (firstNL < size && view[firstNL] !== 10) firstNL++;
    
    const headerLine = Buffer.from(view.slice(0, firstNL)).toString();
    const headers = headerLine.split(delimiter).map(h => h.trim().replace(/^"|"$/g, ''));

    return {
      sharedBuffer: view.buffer,
      headers,
      fileSize: size,
      type: 'csv',
      delimiter
    };
  }

  /**
   * Prepara JSON o NDJSON para procesamiento paralelo.
   */
  _prepareJSON(view, size) {
    // Si es NDJSON (JSON por líneas), lo tratamos casi como un CSV
    const isNDJSON = this.type === 'ndjson' || view[0] === 123; // '{'
    
    let headers = [];
    if (isNDJSON) {
      // Intentar extraer headers del primer objeto
      let firstEnd = 0;
      while (firstEnd < size && view[firstEnd] !== 10) firstEnd++;
      try {
        const firstObj = JSON.parse(Buffer.from(view.slice(0, firstEnd)).toString());
        headers = Object.keys(firstObj);
      } catch (e) {
        headers = ['data']; // Fallback
      }
    }

    return {
      sharedBuffer: view.buffer,
      headers,
      fileSize: size,
      type: this.type,
      isNDJSON
    };
  }
}