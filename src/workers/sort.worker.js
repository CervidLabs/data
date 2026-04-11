import { parentPort, workerData } from 'worker_threads';
import fs from 'fs';

/**
 * Fast Parse: Convierte bytes a número sin crear strings.
 */
function fastParse(buffer, start, end) {
    let num = 0, sign = 1, i = start;
    if (buffer[i] === 45) { sign = -1; i++; }
    while (i < end && buffer[i] !== 46) {
        const digit = buffer[i] - 48;
        if (digit >= 0 && digit <= 9) num = num * 10 + digit;
        i++;
    }
    if (i < end && buffer[i] === 46) {
        i++;
        let frac = 0.1;
        while (i < end) {
            const digit = buffer[i] - 48;
            if (digit >= 0 && digit <= 9) {
                num += digit * frac;
                frac /= 10;
            }
            i++;
        }
    }
    return num * sign;
}

async function fastSort() {
    const { 
        filePath, startByte, endByte, 
        sortColIndex, ascending, delimiter = ',' 
    } = workerData;
    
    const delimCode = delimiter.charCodeAt(0);
    
    // Solo guardamos la LLAVE y el ÍNDICE.
    // Usamos TypedArrays si conocemos el tamaño, o un buffer de pares.
    const keys = [];
    const rowIndices = [];

    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath, { start: startByte, end: endByte });
        let globalRowIndex = 0;
        let currentCol = 0;
        let fieldStart = 0;
        let leftover = Buffer.alloc(0);

        stream.on('data', (chunk) => {
            const buffer = leftover.length > 0 ? Buffer.concat([leftover, chunk]) : chunk;
            const len = buffer.length;
            let lastNewline = 0;

            for (let i = 0; i < len; i++) {
                const byte = buffer[i];

                if (byte === delimCode || byte === 10) {
                    // Solo parseamos la columna por la que queremos ordenar
                    if (currentCol === sortColIndex) {
                        keys.push(fastParse(buffer, fieldStart, i));
                        rowIndices.push(globalRowIndex);
                    }

                    fieldStart = i + 1;
                    currentCol++;

                    if (byte === 10) {
                        currentCol = 0;
                        globalRowIndex++;
                        lastNewline = i;
                    }
                }
            }
            leftover = buffer.slice(lastNewline + 1);
            fieldStart = 0;
        });

        stream.on('end', () => {
            // Ordenamos solo los índices basados en las llaves extraídas
            // Esto es muchísimo más rápido que ordenar objetos completos
            const p = Array.from(rowIndices.keys());
            p.sort((a, b) => {
                const va = keys[a];
                const vb = keys[b];
                if (va === vb) return 0;
                return ascending ? va - vb : vb - va;
            });

            const sortedIndices = p.map(i => rowIndices[i]);
            const sortedKeys = p.map(i => keys[i]);

            parentPort.postMessage({ 
                type: 'sorted_indices', 
                indices: sortedIndices,
                keys: sortedKeys 
            });
            resolve();
        });

        stream.on('error', reject);
    });
}

fastSort().catch(err => {
    parentPort.postMessage({ type: 'error', error: err.message });
    process.exit(1);
});