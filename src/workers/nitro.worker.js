import { parentPort, workerData } from 'worker_threads';

const { sharedBuffer, start, end } = workerData;
const data = new Uint8Array(sharedBuffer);

// Función de parseo ultra-optimizada (sin strings)
function fastFloat(buf, s, e) {
    let res = 0, mul = 1, i = s;
    if (buf[i] === 45) { mul = -1; i++; }
    while (i < e && buf[i] !== 46) {
        res = res * 10 + (buf[i] - 48);
        i++;
    }
    if (i < e && buf[i] === 46) {
        i++;
        let f = 0.1;
        while (i < e) {
            res += (buf[i] - 48) * f;
            f /= 10;
            i++;
        }
    }
    return res * mul;
}

function process() {
    let cursor = start;
    let fieldStart = start;
    let col = 0;
    
    // Aquí es donde Octopus vuela: escaneo lineal de RAM
    for (let i = start; i < end; i++) {
        const b = data[i];
        if (b === 44 || b === 10) { // Coma o Salto de línea
            // Escribir el dato directamente
            // (Necesitarás pasar los SharedArrayBuffers de las COLUMNAS también)
            fieldStart = i + 1;
            if (b === 10) col = 0;
            else col++;
        }
    }
}