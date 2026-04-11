import fs from "fs";
import { Worker } from "worker_threads";
import os from "os";
import { DataFrame } from "./DataFrame.js";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export class Octopus {
	/**
	 * Punto de entrada universal. Detecta el formato y elige el motor.
	 */
	static async read(filePath, options = {}) {
		const extension = filePath.split(".").pop().toLowerCase();

		// Si es JSON, usamos una lógica de carga distinta
		if (extension === "json" || options.type === "json") {
			return await this._readJSON(filePath, options);
		}

		// Por defecto, asumimos CSV (tu lógica Nitro actual)
		return await this._readCSV(filePath, options);
	}

	/**
	 * Motor Nitro para CSV (Tu código actual optimizado)
	 */
	static async _readCSV(filePath, options = {}) {
		const stats = fs.statSync(filePath);
		const fileSize = stats.size;
		const fd = fs.openSync(filePath, "r");

		// Analizar cabecera
		const headerBuffer = Buffer.alloc(10000);
		fs.readSync(fd, headerBuffer, 0, 10000, 0);
		const firstLine = headerBuffer.toString().split("\n")[0];
		const headers = firstLine.trim().split(",");
		const totalCols = headers.length;

		// Configuración Workers
		const numWorkers = options.workers || os.cpus().length;
		const capacity = options.indexerCapacity || 10_000_000;

		const sharedBuffer = new SharedArrayBuffer(fileSize);
		fs.readSync(fd, new Uint8Array(sharedBuffer), 0, fileSize, 0);
		fs.closeSync(fd);

		const colBuffers = headers.map(() => new SharedArrayBuffer(capacity * 8));
		const useOffsets = options.useOffsets !== false;
		let offsetBuffer = useOffsets
			? new SharedArrayBuffer(capacity * totalCols * 2 * 4)
			: null;

		const chunkSize = Math.floor(fileSize / numWorkers);
		const promises = [];
		let currentRow = 0;

		for (let i = 0; i < numWorkers; i++) {
			const start = i * chunkSize;
			const end = i === numWorkers - 1 ? fileSize : (i + 1) * chunkSize;

			promises.push(
				new Promise((resolve) => {
					const workerPath = path.join(__dirname,`..`, 'workers', "ingest.worker.js");
					
					const worker = new Worker(workerPath, {
						workerData: {
							sharedBuffer,
							offsetBuffer,
							colBuffers,
							start,
							end,
							startRow: Math.floor(capacity / numWorkers) * i,
							headers,
						},
					});
					worker.on("message", (msg) => {
						if (msg.type === "done") {
							currentRow += msg.rowCount;
							resolve();
						}
					});
				}),
			);
		}

		await Promise.all(promises);

		const columns = {};
		headers.forEach((h, i) => (columns[h] = new Float64Array(colBuffers[i])));

		return new DataFrame({
			columns,
			rowCount: currentRow,
			headers,
			originalBuffer: new Uint8Array(sharedBuffer),
			offsets: offsetBuffer ? new Int32Array(offsetBuffer) : null,
			colMap: Object.fromEntries(headers.map((h, i) => [h, i])),
		});
	}

	/**
	 * Motor para JSON (Lógica de aplanamiento para Nobel/Amazon)
	 */
	static async _readJSON(filePath, options = {}) {
		console.log("🐙 Octopus procesando JSON...");
		const raw = fs.readFileSync(filePath, "utf8");
		let data = JSON.parse(raw);

		// Auto-detección de raíz (ej: "prizes" en Nobel)
		if (!Array.isArray(data)) {
			const rootKey = Object.keys(data).find((key) => Array.isArray(data[key]));
			data = rootKey ? data[rootKey] : [data];
		}

		// Aplanamiento recursivo (Flattening)
		const flatten = (obj, prefix = "") => {
			let res = {};
			for (const [key, val] of Object.entries(obj)) {
				if (val && typeof val === "object" && !Array.isArray(val)) {
					Object.assign(res, flatten(val, `${prefix}${key}_`));
				} else {
					res[`${prefix}${key}`] = val;
				}
			}
			return res;
		};

		// Si hay arrays anidados (como laureates), multiplicamos filas
		// In _readJSON logic:
		const rows = data.flatMap((item) => {
			const nestedArrayKey = Object.keys(item).find((k) =>
				Array.isArray(item[k]),
			);
			if (nestedArrayKey) {
				// Create base object WITHOUT the original array key
				const { [nestedArrayKey]: _, ...rest } = item;
				const base = flatten(rest);

				return item[nestedArrayKey].map((sub) => ({
					...base,
					...flatten(sub),
				}));
			}
			return flatten(item);
		});

		// Convertir a estructura de columnas de Octopus
		const headers = Object.keys(rows[0] || {});
		const columns = {};
		headers.forEach((h) => {
			const sample = rows[0][h];
			if (typeof sample === "number") {
				columns[h] = new Float64Array(rows.map((r) => r[h] || 0));
			} else {
				columns[h] = rows.map((r) => r[h]); // Mantenemos como array de strings/objetos
			}
		});

		return new DataFrame({
			columns,
			rowCount: rows.length,
			headers,
		});
	}
}
