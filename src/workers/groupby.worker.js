import { parentPort, workerData } from "worker_threads";
import fs from "fs";

/**
 * Fast Parse: Convierte bytes a número sin crear strings intermedios.
 */
function fastParse(buffer, start, end) {
	let num = 0,
		sign = 1,
		i = start;
	if (buffer[i] === 45) {
		sign = -1;
		i++;
	}
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

async function fastGroupBy() {
	const {
		filePath,
		startByte,
		endByte,
		groupColIndices,
		aggColIndex,
		aggType,
		delimiter = ",",
	} = workerData;

	const delimCode = delimiter.charCodeAt(0);
	const groups = new Map();

	return new Promise((resolve, reject) => {
		const stream = fs.createReadStream(filePath, {
			start: startByte,
			end: endByte,
		});
		let currentCol = 0;
		let fieldStart = 0;
		let leftover = Buffer.alloc(0);

		// Temporales para la fila actual
		let currentKeyParts = new Array(groupColIndices.length);
		let currentAggVal = 0;

		stream.on("data", (chunk) => {
			const buffer =
				leftover.length > 0 ? Buffer.concat([leftover, chunk]) : chunk;
			const len = buffer.length;
			let lastNewline = 0;

			for (let i = 0; i < len; i++) {
				const byte = buffer[i];

				if (byte === delimCode || byte === 10) {
					// 1. Extraer clave si la columna es de agrupación
					const keyIdx = groupColIndices.indexOf(currentCol);
					if (keyIdx !== -1) {
						// Aquí creamos el string solo para la clave necesaria
						currentKeyParts[keyIdx] = buffer.toString("utf8", fieldStart, i);
					}

					// 2. Extraer valor si es la columna de agregación
					if (currentCol === aggColIndex) {
						currentAggVal = fastParse(buffer, fieldStart, i);
					}

					fieldStart = i + 1;
					currentCol++;

					if (byte === 10) {
						// Fin de línea: Procesar agregación
						const key = currentKeyParts.join("|");
						if (!groups.has(key)) {
							groups.set(key, {
								count: 0,
								sum: 0,
								min: Infinity,
								max: -Infinity,
							});
						}

						const g = groups.get(key);
						g.count++;
						g.sum += currentAggVal;
						if (currentAggVal < g.min) g.min = currentAggVal;
						if (currentAggVal > g.max) g.max = currentAggVal;

						currentCol = 0;
						lastNewline = i;
					}
				}
			}
			leftover = buffer.slice(lastNewline + 1);
			fieldStart = 0;
		});

		stream.on("end", () => {
			// Convertir Map a objeto plano para el postMessage
			const results = Array.from(groups.entries()).map(([key, stats]) => ({
				key,
				...stats,
				mean: stats.sum / stats.count,
			}));
			parentPort.postMessage({ type: "groupby", data: results });
			resolve();
		});

		stream.on("error", reject);
	});
}

fastGroupBy().catch((err) => {
	parentPort.postMessage({ type: "error", error: err.message });
	process.exit(1);
});
