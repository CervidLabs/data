import { parentPort, workerData } from 'worker_threads';
import fs from 'fs';

/**
 * Fast Parse: Convierte bytes a número sin strings.
 */
function fastParse(buffer, start, end) {
    let num = 0, sign = 1, i = start;
    if (buffer[i] === 45) { sign = -1; i++; }
    while (i < end && buffer[i] !== 46) {
        num = num * 10 + (buffer[i] - 48);
        i++;
    }
    if (i < end && buffer[i] === 46) {
        i++;
        let frac = 0.1;
        while (i < end) {
            num += (buffer[i] - 48) * frac;
            frac /= 10;
            i++;
        }
    }
    return num * sign;
}

async function fastFilter() {
    const { 
        filePath, startByte, endByte, 
        delimiter = ',', headers, filterConfig 
    } = workerData;
    
    const delimCode = delimiter.charCodeAt(0);
    const indices = [];
    
    // filterConfig debe ser un objeto: { fieldIndex: number, value: number, operator: 'gt'|'lt'|'eq' }
    // Esto evita usar eval() y es mucho más rápido.
    const { fieldIndex, targetValue, operator } = filterConfig;

    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath, { start: startByte, end: endByte });
        let globalLineNumber = 0;
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
                    // Solo parseamos si es la columna que nos interesa filtrar
                    if (currentCol === fieldIndex) {
                        const val = fastParse(buffer, fieldStart, i);
                        
                        // Lógica de filtrado ultra-rápida
                        let match = false;
                        if (operator === 'gt') match = val > targetValue;
                        else if (operator === 'lt') match = val < targetValue;
                        else if (operator === 'eq') match = val === targetValue;

                        if (match) indices.push(globalLineNumber);
                    }

                    fieldStart = i + 1;
                    currentCol++;

                    if (byte === 10) {
                        currentCol = 0;
                        globalLineNumber++;
                        lastNewline = i;
                    }
                }
            }

            leftover = buffer.slice(lastNewline + 1);
            fieldStart = 0;
        });

        stream.on('end', () => {
            parentPort.postMessage({ type: 'indices', indices });
            resolve();
        });

        stream.on('error', reject);
    });
}

fastFilter().catch(err => {
    parentPort.postMessage({ type: 'error', error: err.message });
    process.exit(1);
});