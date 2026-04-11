import fs from "fs";
import readline from "readline";
import { DataFrame } from "../core/DataFrame.js";

/**
 * Lee un archivo TXT delimitado y devuelve un DataFrame
 * @param {string} filePath - Ruta del archivo
 * @param {Object} options - Opciones de configuración
 * @param {string} options.delimiter - Delimitador (por defecto '\t' para TSV)
 * @param {boolean} options.header - Si la primera fila es header (por defecto true)
 * @param {string} options.encoding - Codificación (por defecto 'utf8')
 * @param {number} options.chunkSize - Tamaño del batch (por defecto 10000)
 * @param {Array} options.fixedWidths - Para archivos de ancho fijo: [10, 20, 15, ...]
 * @param {Array} options.columnNames - Nombres de columnas para ancho fijo
 * @returns {Promise<DataFrame>}
 */
export async function readTXT(filePath, options = {}) {
	const {
		delimiter = "\t",
		header = true,
		encoding = "utf8",
		chunkSize = 10000,
		fixedWidths = null,
		columnNames = null,
	} = options;

	const df = new DataFrame();
	df.filePath = filePath;
	df.fileType = "txt";

	const stats = fs.statSync(filePath);
	const isLargeFile = stats.size > 100 * 1024 * 1024; // > 100MB

	if (fixedWidths) {
		// Archivo de ancho fijo
		await readFixedWidth(filePath, df, {
			fixedWidths,
			columnNames,
			encoding,
			chunkSize,
		});
	} else {
		// Archivo delimitado (TSV, pipe, etc.)
		await readDelimited(filePath, df, {
			delimiter,
			header,
			encoding,
			chunkSize,
			isLargeFile,
		});
	}

	console.log(
		`✅ TXT cargado: ${df.rowCount.toLocaleString()} filas, ${Object.keys(df.columns).length} columnas`,
	);

	return df;
}

/**
 * Lee archivo delimitado (TSV, CSV con otro delimiter, etc.)
 */
async function readDelimited(filePath, df, options) {
	const { delimiter, header, encoding, chunkSize, isLargeFile } = options;

	const stream = fs.createReadStream(filePath, { encoding });
	const rl = readline.createInterface({
		input: stream,
		crlfDelay: Infinity,
	});

	let headers = [];
	let rowCount = 0;
	let isFirstLine = true;
	let batch = [];

	for await (const line of rl) {
		if (!line.trim()) continue;

		const values = parseDelimitedLine(line, delimiter);

		// Primera línea: headers
		if (isFirstLine && header) {
			headers = values.map((v) => cleanValue(v).replace(/[^a-zA-Z0-9_]/g, "_"));
			df.columns = {};
			headers.forEach((col) => {
				df.columns[col] = [];
			});
			isFirstLine = false;
			continue;
		}

		// Sin header: usar índices
		if (isFirstLine && !header) {
			headers = values.map((_, i) => `col_${i}`);
			df.columns = {};
			headers.forEach((col) => {
				df.columns[col] = [];
			});
			isFirstLine = false;
		}

		// Procesar fila
		headers.forEach((header, i) => {
			let value = values[i] || "";
			value = cleanValue(value);

			// Intentar parsear números
			if (
				value !== "" &&
				!isNaN(value) &&
				value !== "null" &&
				value !== "undefined"
			) {
				const num = Number(value);
				if (!isNaN(num) && value.trim() !== "") {
					value = num;
				}
			}

			df.columns[header].push(value);
		});

		rowCount++;

		// Limpiar batch periódicamente (liberar memoria)
		if (batch.length >= chunkSize) {
			batch = [];
		}
	}

	df.rowCount = rowCount;
}

/**
 * Lee archivo de ancho fijo
 */
async function readFixedWidth(filePath, df, options) {
	const { fixedWidths, columnNames, encoding, chunkSize } = options;

	const stream = fs.createReadStream(filePath, { encoding });
	const rl = readline.createInterface({
		input: stream,
		crlfDelay: Infinity,
	});

	// Determinar headers
	let headers = columnNames || fixedWidths.map((_, i) => `col_${i}`);

	df.columns = {};
	headers.forEach((col) => {
		df.columns[col] = [];
	});

	let rowCount = 0;
	let isFirstLine = true;
	let skipHeader = columnNames ? false : true;

	for await (const line of rl) {
		if (!line.trim()) continue;

		// Saltar header si no se proporcionaron nombres
		if (skipHeader && isFirstLine) {
			isFirstLine = false;
			continue;
		}

		// Parsear por ancho fijo
		let start = 0;
		const values = [];

		for (const width of fixedWidths) {
			const value = line.substring(start, start + width).trim();
			values.push(value);
			start += width;
		}

		// Agregar a columnas
		headers.forEach((header, i) => {
			let value = values[i] || "";

			// Intentar parsear números
			if (
				value !== "" &&
				!isNaN(value) &&
				value !== "null" &&
				value !== "undefined"
			) {
				const num = Number(value);
				if (!isNaN(num) && value.trim() !== "") {
					value = num;
				}
			}

			df.columns[header].push(value);
		});

		rowCount++;

		if (rowCount % chunkSize === 0) {
			// Dejar respirar al event loop
			await new Promise((resolve) => setImmediate(resolve));
		}
	}

	df.rowCount = rowCount;
}

/**
 * Parsea línea delimitada respetando comillas
 */
function parseDelimitedLine(line, delimiter) {
	const result = [];
	let current = "";
	let inQuotes = false;
	let i = 0;

	while (i < line.length) {
		const char = line[i];

		if (char === '"') {
			inQuotes = !inQuotes;
			i++;
			continue;
		}

		if (char === delimiter && !inQuotes) {
			result.push(current);
			current = "";
			i++;
			continue;
		}

		current += char;
		i++;
	}

	result.push(current);
	return result;
}

/**
 * Limpia el valor (remueve espacios y comillas)
 */
function cleanValue(value) {
	value = value.trim();
	if (value.startsWith('"') && value.endsWith('"')) {
		value = value.slice(1, -1);
	}
	return value;
}
