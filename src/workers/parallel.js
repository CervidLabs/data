import { Worker } from "worker_threads";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export class ParallelExecutor {
	constructor(filePath, options = {}) {
		this.filePath = filePath;
		this.headers = options.headers || [];
		this.numWorkers = options.numWorkers || 4;
		this.transforms = options.transforms || [];
	}

	/**
	 * Estima las filas para dimensionar los buffers.
	 */
	estimateRows(view) {
		let count = 0;
		for (let i = 0; i < view.length; i++) {
			if (view[i] === 10) count++;
		}
		return count;
	}

	/**
	 * Divide el buffer en trozos equitativos para los workers.
	 */
	getBufferChunks(view) {
		const size = view.length;
		const chunkSize = Math.floor(size / this.numWorkers);
		const chunks = [];

		for (let i = 0; i < this.numWorkers; i++) {
			let start = i * chunkSize;
			let end = i === this.numWorkers - 1 ? size : (i + 1) * chunkSize;

			// Ajustar al inicio de una línea
			if (i > 0) {
				while (start < size && view[start - 1] !== 10) start++;
			}
			// Ajustar al final de una línea
			while (end < size && view[end - 1] !== 10) end++;

			if (start < end) chunks.push({ start, end });
		}
		return chunks;
	}

	async executeIngest(meta) {
		// 1. Sincronización de Headers (Originales + Virtuales)
		this.headers = meta.headers;
		const sharedBuffer = meta.sharedBuffer;
		const view = new Uint8Array(sharedBuffer);
		const rowCount = this.estimateRows(view);
		const chunks = this.getBufferChunks(view);

		// 2. Reserva de memoria compartida (8 bytes por celda para Float64)
		// Creamos un buffer por cada columna en los headers finales
		const colBuffers = this.headers.map(
			() => new SharedArrayBuffer(rowCount * 8),
		);

		// 3. Preparación del Pipeline de Transformación
		// Calculamos los índices UNA SOLA VEZ para que el worker no busque strings
		// src/workers/parallel.js -> executeIngest

		// ...
		// 3. Preparación del Pipeline de Transformación
		const compiledTransforms = this.transforms.map((t) => {
			// 🛡️ Defensa: Aseguramos que inputs sea un array
			const inputKeys = t.inputs || [];

			return {
				name: t.name,
				targetIdx: this.headers.indexOf(t.name),
				// Mapeamos los nombres de las columnas a sus índices numéricos
				inputIndices: inputKeys.map((inputName) => {
					const idx = this.headers.indexOf(inputName);
					if (idx === -1) {
						console.warn(
							`⚠️ Advertencia: Columna de entrada "${inputName}" no encontrada para ${t.name}`,
						);
					}
					return idx;
				}),
				formulaStr: t.formula,
				argNames: inputKeys,
			};
		});

		const workerPath = path.resolve(__dirname, "ingest.worker.js");
		let currentStartRow = 0;
		const workers = [];

		// 4. Lanzamiento de Workers
		for (const chunk of chunks) {
			// Contar filas en este chunk para el offset de escritura
			let rowsInChunk = 0;
			for (let j = chunk.start; j < chunk.end; j++) {
				if (view[j] === 10) rowsInChunk++;
			}

			const worker = new Worker(workerPath, {
				workerData: {
					sharedBuffer,
					colBuffers,
					start: chunk.start,
					end: chunk.end,
					startRow: currentStartRow,
					transforms: compiledTransforms,
					headers: this.headers,
				},
			});

			workers.push(worker);
			currentStartRow += rowsInChunk;
		}

		// 5. Orquestación y espera
		await Promise.all(
			workers.map((worker, i) => {
				return new Promise((resolve, reject) => {
					worker.on("message", (msg) => {
						if (msg.type === "done") resolve();
					});
					worker.on("error", (err) => reject(`Worker ${i} falló: ${err}`));
					worker.on("exit", (code) => {
						if (code !== 0) reject(`Worker ${i} terminó con código ${code}`);
					});
				});
			}),
		);

		workers.forEach((w) => w.terminate());

		// 6. Mapeo final del objeto de columnas para el DataFrame
		const columns = {};
		this.headers.forEach((header, index) => {
			columns[header] = new Float64Array(colBuffers[index]);
		});

		return { columns, rowCount };
	}
}
