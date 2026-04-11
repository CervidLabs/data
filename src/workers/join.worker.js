import { parentPort, workerData } from 'worker_threads';
import fs from 'fs';

/**
 * Fast Parse: Convierte bytes a número para comparación de llaves.
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

async function fastJoin() {
    const { 
        filePath, startByte, endByte, hashTable, 
        rightColIndex, how, delimiter = ',' 
    } = workerData;
    
    // hashTable ya viene como un Map de una fase previa: { llave: [índices_izquierdos] }
    const lookupMap = hashTable instanceof Map ? hashTable : new Map(hashTable);
    const delimCode = delimiter.charCodeAt(0);
    
    // Usaremos TypedArrays para los resultados si es posible, 
    // pero para joins 1:N usaremos un buffer plano de índices.
    const leftIndices = [];
    const rightRowIndices = [];

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
                    // Solo parseamos la columna que es la llave del Join (Right Table)
                    if (currentCol === rightColIndex) {
                        const key = fastParse(buffer, fieldStart, i);
                        const matches = lookupMap.get(key);

                        if (matches) {
                            for (let j = 0; j < matches.length; j++) {
                                leftIndices.push(matches[j]);
                                rightRowIndices.push(globalRowIndex);
                            }
                        } else if (how === 'right' || how === 'outer') {
                            // Si no hay match pero es un Right Join
                            leftIndices.push(-1); // Representa NULL
                            rightRowIndices.push(globalRowIndex);
                        }
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
            // Enviamos solo los pares de índices. 
            // La "reconstrucción" de la fila se hace al final, solo si es necesario.
            parentPort.postMessage({ 
                type: 'join_indices', 
                leftIndices, 
                rightRowIndices 
            });
            resolve();
        });

        stream.on('error', reject);
    });
}

fastJoin().catch(err => {
    parentPort.postMessage({ type: 'error', error: err.message });
    process.exit(1);
});