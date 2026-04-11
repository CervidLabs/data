import { parentPort, workerData } from 'worker_threads';

const { sharedBuffer, colBuffers, start, end, startRow } = workerData;

const data = new Uint8Array(sharedBuffer);
// Convertir los buffers crudos a vistas Float64
const columns = colBuffers.map(buffer => new Float64Array(buffer));

function fastParse(buf, s, e) {
    let num = 0, sign = 1, i = s;
    if (buf[i] === 45) { sign = -1; i++; }
    while (i < e && buf[i] !== 46) {
        num = num * 10 + (buf[i] - 48);
        i++;
    }
    if (i < e && buf[i] === 46) {
        i++;
        let frac = 0.1;
        while (i < e) {
            num += (buf[i] - 48) * frac;
            frac /= 10;
            i++;
        }
    }
    return num * sign;
}

function process() {
    let cursor = start;
    // IMPORTANTE: El primer worker debe saltarse la línea de nombres (headers)
    if (start === 0) {
        while (cursor < end && data[cursor] !== 10) cursor++;
        cursor++; 
    }

    let row = startRow;
    let col = 0;
    let fieldStart = cursor;

    for (let i = cursor; i < end; i++) {
        const byte = data[i];

        // 44 = coma, 10 = \n
        if (byte === 44 || byte === 10) {
            if (i > fieldStart && columns[col]) {
                columns[col][row] = fastParse(data, fieldStart, i);
            }
            
            fieldStart = i + 1;
            col++;

            if (byte === 10) {
                col = 0;
                row++;
            }
        }
    }
    
    // ESTA LÍNEA ES VITAL: Si no se envía, el Executor no sabe que terminó
    parentPort.postMessage({ type: 'done' });
}

process();