import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Clase Series: El pilar columnar de Octopus.
 * Optimizada para no usar objetos Map y trabajar sobre memoria plana.
 */
export class Series {
    constructor(name, data, type, indexer = null, mask = null) {
        this.name = name;
        this.data = data;       // TypedArray (Float64Array o Int32Array)
        this.type = type;       // 'float64' | 'int32'
        this.indexer = indexer; // Instancia de StringIndexer (TypedArray Hash)
        this.mask = mask;       // Uint8Array para filtros
    }

    /**
     * Obtiene el valor en un índice.
     * Si tiene un indexer, traduce el ID numérico a String (ASIN) al vuelo.
     */
    get(index) {
        const val = this.data[index];

        // Si la columna es de productos (IDs), usamos el indexer para recuperar el nombre
        if (this.indexer && typeof this.indexer.getStringById === 'function') {
            return this.indexer.getStringById(val);
        }

        return val;
    }

    /**
     * Devuelve la longitud de la serie
     */
    get length() {
        return this.data.length;
    }

    /**
     * Crea una porción de la serie (usado por .limit())
     * Mantiene la referencia al indexer para que la traducción siga funcionando.
     */
    slice(start, end) {
        const slicedData = this.data.slice(start, end);
        return new Series(this.name, slicedData, this.type, this.indexer, this.mask);
    }

    /**
     * Método estático para reconstruir Series desde buffers crudos
     */
    static fromRawBuffer(name, data, type, indexer = null, mask = null) {
        return new Series(name, data, type, indexer, mask);
    }

    /**
     * Formatea resultados de agregación (usado internamente por DataFrame)
     */
    static formatResults(results, indexer, opName) {
        return {
            data: results,
            show: (n = 10) => {
                const entries = Object.entries(results);
                console.log(`\n📊 Operación [${opName.toUpperCase()}] sobre ${entries.length.toLocaleString()} grupos.`);
                
                const table = entries
                    .map(([id, val]) => {
                        const numericId = parseInt(id);
                        // Traducimos el ID a ASIN usando el indexer de memoria plana
                        const productName = (indexer) 
                            ? (indexer.getStringById(numericId) || `ID: ${id}`) 
                            : `ID: ${id}`;
                            
                        return {
                            product: productName,
                            [opName]: parseFloat(val.toFixed(2))
                        };
                    })
                    .filter(item => item.product !== "ID: 0" && !item.product.startsWith("ID: -"))
                    .sort((a, b) => b[opName] - a[opName])
                    .slice(0, n);
                
                console.table(table);
            }
        };
    }
}