import { parentPort, workerData } from "worker_threads";

const {
	sharedBuffer,
	offsetBuffer,
	colBuffers,
	start,
	end,
	startRow,
	headers,
} = workerData;

const view = new Uint8Array(sharedBuffer);
const offsetView = offsetBuffer ? new Int32Array(offsetBuffer) : null;
const columns = colBuffers.map((b) => (b ? new Float64Array(b) : null));
const totalCols = headers.length;

function fastParseFloat(buffer, start, end) {
	if (start >= end) return 0;
	let val = 0,
		divisor = 1,
		dotSeen = false,
		i = start,
		sign = 1;
	if (buffer[i] === 45) {
		sign = -1;
		i++;
	}
	for (; i < end; i++) {
		const b = buffer[i];
		if (b === 46) {
			dotSeen = true;
			continue;
		}
		if (b >= 48 && b <= 57) {
			val = val * 10 + (b - 48);
			if (dotSeen) divisor *= 10;
		}
	}
	return (val / divisor) * sign;
}

function process() {
	let pos = start;
	let rowIdx = startRow;
	let inQuotes = false;

	// 1. Sincronización inicial
	if (start === 0) {
		while (pos < end && view[pos] !== 10) pos++;
		pos++;
	}

	while (pos < end) {
		let col = 0;
		let fieldStart = pos;
		let rowFinished = false;

		while (pos < end) {
			const byte = view[pos];

			// Manejo de comillas dobles (byte 34)
			if (byte === 34) {
				// Verificación de comillas escapadas "" (estándar CSV)
				if (inQuotes && view[pos + 1] === 34) {
					pos++; // Saltamos la secuencia escapada
				} else {
					inQuotes = !inQuotes;
				}
			}

			// Detectar delimitadores solo si no estamos dentro de una celda protegida
			if (!inQuotes) {
				if (byte === 44 || byte === 10) {
					// Coma o Salto de línea
					if (col < totalCols) {
						// Guardar valor numérico
						if (columns[col]) {
							columns[col][rowIdx] = fastParseFloat(view, fieldStart, pos);
						}

						// Guardar Offsets (Strings)
						if (offsetView) {
							const offsetPos = (rowIdx * totalCols + col) * 2;
							let s = fieldStart;
							let e = pos;

							// Limpiar comillas de los bordes
							if (view[s] === 34) s++;
							if (view[e - 1] === 34) e--;
							// Limpiar posible \r (byte 13) de Windows
							if (view[e - 1] === 13) e--;

							offsetView[offsetPos] = s;
							offsetView[offsetPos + 1] = Math.max(s, e);
						}
					}

					if (byte === 10) {
						// Fin de fila
						pos++;
						rowFinished = true;
						break;
					}

					col++;
					pos++;
					fieldStart = pos;
					continue;
				}
			}
			pos++;
		}

		// Manejo de la última línea si el archivo no termina en \n
		if (!rowFinished && pos >= end && col > 0) {
			rowFinished = true;
		}

		if (rowFinished) {
			rowIdx++;
			col = 0;
			// IMPORTANTE: Resetear inQuotes por si una fila quedó mal cerrada
			inQuotes = false;
		}
	}
	parentPort.postMessage({ type: "done", rowCount: rowIdx - startRow });
}

process();
