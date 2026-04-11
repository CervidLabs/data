import { parentPort, workerData } from 'worker_threads';

const { sharedBuffer, colBuffers, start, end, startRow } = workerData;
const data = new Uint8Array(sharedBuffer);
const columns = colBuffers.map(buffer => new Float64Array(buffer));

// Mapa local para evitar colisiones de strings en el mismo hilo
const localStringMap = new Map();

/**
 * Fast Parse optimizado: 
 * Intenta convertir a número, si no puede, devuelve NaN para activar el String Mapping.
 */
function fastParse(buf, s, e) {
    let num = 0, sign = 1, i = s;
    if (buf[i] === 45) { sign = -1; i++; }
    
    // Si el primer carácter no es un número o punto, es un String
    const first = buf[i];
    if (!(first >= 48 && first <= 57) && first !== 46) return NaN;

    while (i < e && buf[i] !== 46) {
        const digit = buf[i] - 48;
        if (digit >= 0 && digit <= 9) num = num * 10 + digit;
        else return NaN; // Carácter no numérico en medio de la cadena
        i++;
    }
    if (i < e && buf[i] === 46) {
        i++;
        let frac = 0.1;
        while (i < e) {
            const digit = buf[i] - 48;
            if (digit >= 0 && digit <= 9) {
                num += digit * frac;
                frac /= 10;
            } else return NaN;
            i++;
        }
    }
    return num * sign;
}

/**
 * Genera un ID numérico a partir de un String mediante hashing rápido.
 * Esto permite guardar texto en un Float64Array sin perder el orden.
 */
function stringToId(buf, s, e) {
    let hash = 0;
    for (let i = s; i < e; i++) {
        hash = (hash << 5) - hash + buf[i];
        hash |= 0; // Convertir a 32bit signed int
    }
    return hash;
}

function process() {
    let cursor = start;
    if (start === 0) {
        while (cursor < end && data[cursor] !== 10) cursor++;
        cursor++; 
    }

    let row = startRow;
    let col = 0;
    let fieldStart = cursor;
    const stringsFound = []; // Para reportar al Pool principal si fuera necesario

    for (let i = cursor; i < end; i++) {
        const byte = data[i];

        if (byte === 44 || byte === 10) { // Coma o Salto de línea
            if (i > fieldStart && columns[col]) {
                const val = fastParse(data, fieldStart, i);
                
                if (!isNaN(val)) {
                    columns[col][row] = val;
                } else {
                    // Es un STRING: Guardamos el HASH como ID
                    const id = stringToId(data, fieldStart, i);
                    columns[col][row] = id;
                    
                    // Opcional: Podríamos enviar el texto original de vuelta para el Pool
                    // const text = Buffer.from(data.subarray(fieldStart, i)).toString();
                }
            }
            
            fieldStart = i + 1;
            col++;

            if (byte === 10) {
                col = 0;
                row++;
            }
        }
    }
    
    parentPort.postMessage({ type: 'done' });
}

process();