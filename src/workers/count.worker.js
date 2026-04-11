import { parentPort, workerData } from "worker_threads";
import fs from "fs";

/**
 * FAST PARSE: Convierte bytes a número sin crear strings.
 * El secreto de la velocidad de Octopus.
 */
function fastParse(buffer, start, end) {
	let num = 0,
		sign = 1,
		i = start;
	if (buffer[i] === 45) {
		sign = -1;
		i++;
	} // '-'

	while (i < end && buffer[i] !== 46) {
		// '.'
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

async function fastIngest() {
	const { filePath, startByte, endByte, delimiter = "," } = workerData;
	const delimCode = delimiter.charCodeAt(0);

	// Necesitas pasar los SharedArrayBuffers en workerData desde parallel.js
	// Ejemplo: sharedColumns: [Float64Array, Float64Array...]
	const { sharedColumns } = workerData;

	return new Promise((resolve, reject) => {
		const stream = fs.createReadStream(filePath, {
			start: startByte,
			end: endByte,
		});
		let row = 0;
		let col = 0;
		let fieldStart = 0;
		let leftover = Buffer.alloc(0);

		stream.on("data", (chunk) => {
			// Unir con lo que sobró del chunk anterior (bordes de línea)
			const buffer =
				leftover.length > 0 ? Buffer.concat([leftover, chunk]) : chunk;
			const len = buffer.length;
			let lastNewline = 0;

			// Escáner de Bytes
			for (let i = 0; i < len; i++) {
				const byte = buffer[i];

				if (byte === delimCode || byte === 10) {
					// Coma o Salto de línea
					if (sharedColumns && sharedColumns[col]) {
						sharedColumns[col][row] = fastParse(buffer, fieldStart, i);
					}

					fieldStart = i + 1;
					col++;

					if (byte === 10) {
						// Fin de línea
						col = 0;
						row++;
						lastNewline = i;
					}
				}
			}

			// Guardar el fragmento de línea incompleta para el siguiente chunk
			leftover = buffer.slice(lastNewline + 1);
			fieldStart = 0;
		});

		stream.on("end", () => {
			parentPort.postMessage({ type: "done", rowCount: row });
			resolve();
		});

		stream.on("error", reject);
	});
}

fastIngest().catch((err) => {
	parentPort.postMessage({ type: "error", error: err.message });
	process.exit(1);
});
