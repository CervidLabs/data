import fs from "fs";
import path from "path";
import { CSVExporter } from "../exporters/csv.js";
import { JSONExporter } from "../exporters/json.js";
import { TXTExporter } from "../exporters/txt.js";
import { Column } from "./Column.js";

export class DataFrame {
	constructor(config = {}) {
		// Si config es una instancia, extraemos sus datos
		const data = config instanceof DataFrame ? config : config;

		this.columns = data.columns || {};
		this.rowCount = data.rowCount || 0;

		// 🚨 REGLA DE ORO: Si hay nuevas columnas en 'columns',
		// pero no están en 'headers', las sincronizamos.
		this.headers = data.headers || Object.keys(this.columns);

		// Herencia Nitro
		this.originalBuffer = data.originalBuffer || null;
		this.offsets = data.offsets || null;
		this.numCols = data.numCols || 0;
		this.colMap = data.colMap || null;
		this.metadata = data.metadata || { indexers: {} };
	}
	/**
	 * MÉTODO ESTÁTICO: Convierte un array de objetos [{}, {}]
	 * a una instancia de DataFrame.
	 */
	static fromObjects(data) {
		if (!data || data.length === 0) {
			return new DataFrame({ columns: {}, rowCount: 0, headers: [] });
		}

		const headers = Object.keys(data[0]);
		const columns = {};
		const rowCount = data.length;

		headers.forEach((h) => {
			const sample = data[0][h];

			// Nitro Tip: Si es número, usamos Float64Array para mantener la velocidad
			if (typeof sample === "number") {
				const col = new Float64Array(rowCount);
				for (let i = 0; i < rowCount; i++) {
					col[i] = data[i][h] || 0;
				}
				columns[h] = col;
			} else {
				// Si es texto, usamos Array estándar
				columns[h] = data.map((row) => row[h]);
			}
		});

		return new DataFrame({ columns, rowCount, headers });
	}
	// 🏷️ WITH_LABEL: Sincroniza el Indexer con los bytes originales
	with_label(specs) {
		const newColumns = { ...this.columns };
		const newMetadata = {
			...this.metadata,
			indexers: { ...this.metadata.indexers },
		};

		for (const spec of specs) {
			const { input, indexer } = spec;
			const targetName = `${input}_indexed`;

			// fitTransform usa los offsets para "ver" el texto en el buffer
			const indexedCol = indexer.fitTransform(this, input);

			newColumns[targetName] = indexedCol;
			newMetadata.indexers[input] = indexer;
		}

		return new DataFrame({
			...this,
			columns: newColumns,
			metadata: newMetadata,
		});
	}

	// 🔧 WITH_COLUMNS: Feature Engineering de alta velocidad
	with_columns(specs) {
		const rowCount = this.rowCount;
		for (const spec of specs) {
			const newCol = new Float64Array(rowCount);
			const formula = spec.formula;
			const inputs = spec.inputs.map((name) => this.columns[name]);
			const numInputs = inputs.length;

			// ACCESO DIRECTO: Evitamos crear arrays o usar 'map' dentro del bucle
			if (numInputs === 1) {
				const col0 = inputs[0];
				for (let i = 0; i < rowCount; i++) newCol[i] = formula(col0[i]);
			} else if (numInputs === 2) {
				const col0 = inputs[0],
					col1 = inputs[1];
				for (let i = 0; i < rowCount; i++)
					newCol[i] = formula(col0[i], col1[i]);
			} else if (numInputs === 4) {
				const col0 = inputs[0],
					col1 = inputs[1],
					col2 = inputs[2],
					col3 = inputs[3];
				for (let i = 0; i < rowCount; i++)
					newCol[i] = formula(col0[i], col1[i], col2[i], col3[i]);
			} else {
				// Caso general (fallback)
				for (let i = 0; i < rowCount; i++) {
					const args = new Array(numInputs);
					for (let j = 0; j < numInputs; j++) args[j] = inputs[j][i];
					newCol[i] = formula(...args);
				}
			}
			this.columns[spec.name] = newCol;
		}
		return this;
	}
	show(n = 5) {
		const limit = Math.min(n, this.rowCount);
		const tableData = [];

		for (let i = 0; i < limit; i++) {
			const row = {};
			for (const header of this.headers) {
				let val = this.columns[header][i];

				// Truncamiento inteligente para que la tabla no se rompa
				if (typeof val === "string" && val.length > 20) {
					val = val.substring(0, 17) + "...";
				}
				row[header] = val;
			}
			tableData.push(row);
		}

		console.table(tableData);
	}
	async write(path) {
		const stream = fs.createWriteStream(path);
		// Escribir cabeceras
		stream.write(this.headers.join(",") + "\n");

		// Escribir filas
		for (let i = 0; i < this.rowCount; i++) {
			const row = this.headers.map((h) => {
				const val = this.columns[h][i];
				// Si es un número muy grande o timestamp, lo dejamos como está
				// Si es un float, limitamos decimales para que el CSV no pese tanto
				return Number.isInteger(val) ? val : val.toFixed(4);
			});
			stream.write(row.join(",") + "\n");
		}

		return new Promise((resolve) => stream.on("finish", resolve).end());
	}

	// ==================== AGREGACIONES & UTILIDADES ====================

	groupByRange(colName, targetCol, maxRange) {
		const groupCounts = new Uint32Array(maxRange);
		const groupSums = new Float64Array(maxRange);
		const keys = this.columns[colName];
		const values = this.columns[targetCol];

		if (!keys || !values) return [];

		for (let i = 0; i < this.rowCount; i++) {
			const key = Math.floor(keys[i]);
			// 🛡️ Protección contra IDs fuera de rango (como el 980414)
			if (key >= 0 && key < maxRange) {
				groupCounts[key]++;
				groupSums[key] += values[i];
			}
		}

		return Array.from({ length: maxRange }, (_, i) => ({
			group: i,
			avg: groupCounts[i] > 0 ? groupSums[i] / groupCounts[i] : 0,
		}))
			.filter((r) => r.avg > 0)
			.sort((a, b) => b.avg - a.avg);
	}
	/**
	 * FILTRADO: Retorna un nuevo DataFrame con las filas que cumplen la condición.
	 */
	filter(inputs, predicate) {
		const indices = [];

		// 1. Identificar qué filas pasan el filtro
		for (let i = 0; i < this.rowCount; i++) {
			const rowInputs = inputs.map((h) => this.columns[h][i]);
			if (predicate(...rowInputs)) {
				indices.push(i);
			}
		}

		// 2. Reconstruir columnas con los nuevos índices
		const newColumns = {};
		for (const h of this.headers) {
			const oldCol = this.columns[h];
			const isTyped = oldCol instanceof Float64Array;

			const newCol = isTyped
				? new Float64Array(indices.length)
				: new Array(indices.length);

			for (let j = 0; j < indices.length; j++) {
				newCol[j] = oldCol[indices[j]];
			}
			newColumns[h] = newCol;
		}

		return new DataFrame({
			...this,
			columns: newColumns,
			rowCount: indices.length,
		});
	}
	/**
	 * GROUP BY: Agrupa por una columna y aplica agregaciones.
	 * @param {string} groupCol - Columna para agrupar (ej: 'category')
	 * @param {Object} aggs - Agregaciones (ej: { year: 'count', price: 'mean' })
	 */
	groupBy(groupCol, aggs = {}) {
		const groups = new Map();
		const targetData = this.columns[groupCol];

		// 1. Hash Phase (Igual que antes)
		for (let i = 0; i < this.rowCount; i++) {
			const val = targetData[i];
			if (!groups.has(val)) groups.set(val, []);
			groups.get(val).push(i);
		}

		// 2. Aggregation Phase (Soporta Arrays)
		const resultRows = [];
		for (const [groupVal, indices] of groups.entries()) {
			const row = { [groupCol]: groupVal };

			for (const [colName, ops] of Object.entries(aggs)) {
				const colToAgg = this.columns[colName];
				const values = indices
					.map((idx) => parseFloat(colToAgg[idx]))
					.filter((v) => !isNaN(v));

				// Convertimos a array si el usuario pasó un string solo: 'sum' -> ['sum']
				const operations = Array.isArray(ops) ? ops : [ops];

				operations.forEach((op) => {
					const outName = operations.length > 1 ? `${colName}_${op}` : colName;

					if (op === "sum") row[outName] = values.reduce((a, b) => a + b, 0);
					else if (op === "mean")
						row[outName] = values.reduce((a, b) => a + b, 0) / values.length;
					else if (op === "count") row[outName] = values.length;
					else if (op === "max") row[outName] = Math.max(...values);
					else if (op === "min") row[outName] = Math.min(...values);
				});
			}
			resultRows.push(row);
		}

		return DataFrame.fromObjects(resultRows);
	}
	/**
	 * ORDENAMIENTO: Ordena el DataFrame basado en una columna.
	 */
	sort(columnName, ascending = true) {
		// 1. Crear un array de índices [0, 1, 2... n]
		const indices = Array.from({ length: this.rowCount }, (_, i) => i);
		const targetCol = this.columns[columnName];

		// 2. Ordenar los índices basándonos en los valores de la columna objetivo
		indices.sort((a, b) => {
			const valA = targetCol[a];
			const valB = targetCol[b];

			if (valA < valB) return ascending ? -1 : 1;
			if (valA > valB) return ascending ? 1 : -1;
			return 0;
		});

		// 3. Reordenar todas las columnas usando el nuevo mapa de índices
		const newColumns = {};
		for (const h of this.headers) {
			const oldCol = this.columns[h];
			const isTyped = oldCol instanceof Float64Array;
			const newCol = isTyped
				? new Float64Array(this.rowCount)
				: new Array(this.rowCount);

			for (let j = 0; j < this.rowCount; j++) {
				newCol[j] = oldCol[indices[j]];
			}
			newColumns[h] = newCol;
		}

		return new DataFrame({ ...this, columns: newColumns });
	}
	groupByID(colName, targetCol) {
		return this.groupByRange(colName, targetCol, 300);
	}

	sum(col) {
		const data = this.columns[col];
		if (!data) return 0;
		let total = 0;
		for (let i = 0; i < this.rowCount; i++) total += data[i];
		return total;
	}

	mean(col) {
		return this.rowCount === 0 ? 0 : this.sum(col) / this.rowCount;
	}

	max(col) {
		const data = this.getCol(col);
		if (!data) return null;
		let maxVal = -Infinity;
		const len = this.rowCount;
		for (let i = 0; i < len; i++) {
			if (data[i] > maxVal) maxVal = data[i];
		}
		return maxVal;
	}

	min(col) {
		const data = this.getCol(col);
		if (!data) return null;
		let minVal = Infinity;
		const len = this.rowCount;
		for (let i = 0; i < len; i++) {
			if (data[i] < minVal) minVal = data[i];
		}
		return minVal;
	}

	// ==================== UTILIDADES & EXPORTACIÓN ====================

	info() {
		return {
			rowCount: this.rowCount,
			columnCount: this.headers.length,
			columns: this.headers,
			memoryUsage: `${((this.rowCount * this.headers.length * 8) / 1024 / 1024).toFixed(2)} MB`,
		};
	}

	fromArray(data) {
		if (!data || data.length === 0) return this;
		this.headers = Object.keys(data[0]);
		this.rowCount = data.length;

		this.headers.forEach((h) => {
			this.columns[h] = new Float64Array(
				new SharedArrayBuffer(this.rowCount * 8),
			);
			for (let i = 0; i < this.rowCount; i++) {
				this.columns[h][i] = data[i][h];
			}
		});
		return this;
	}
	/**
	 * Método privado para obtener una fila como objeto.
	 * Útil para toArray, toJSON y exportadores.
	 */
	_getRow(index) {
		const row = {};
		for (const h of this.headers) {
			row[h] = this.columns[h][index];
		}
		return row;
	}
	toArray() {
		const result = [];
		for (let i = 0; i < this.rowCount; i++) {
			result.push(this._getRow(i));
		}
		return result;
	}
	_validatePath(outputPath, requiredExt) {
		const ext = path.extname(outputPath).toLowerCase();
		if (!ext) return outputPath + requiredExt;
		if (ext !== requiredExt) {
			throw new Error(
				`Invalid extension: Output must be ${requiredExt} (received: ${ext})`,
			);
		}
		return outputPath;
	}

	async toCSV(outputPath, options = {}) {
		const validatedPath = this._validatePath(outputPath, ".csv");
		const exporter = new CSVExporter(this, options);
		return await exporter.export(validatedPath);
	}

	async toJSON(outputPath, options = {}) {
		const validatedPath = this._validatePath(outputPath, ".json");
		const exporter = new JSONExporter(this, options);
		return await exporter.export(validatedPath);
	}

	async toTXT(outputPath, options = {}) {
		const validatedPath = this._validatePath(outputPath, ".txt");
		const exporter = new TXTExporter(this, options);
		return await exporter.export(validatedPath);
	}
	/**
	 * DESCRIBE: Genera estadísticas descriptivas de las columnas numéricas.
	 */
	describe() {
		const stats = [];
		for (const h of this.headers) {
			const col = this.columns[h];

			// 1. Convertir a números y filtrar lo que no sea numérico
			const numericValues = Array.from(col)
				.map((v) => parseFloat(v))
				.filter((v) => !isNaN(v));

			// 2. Si no hay números en esta columna, saltar a la siguiente
			if (numericValues.length === 0) continue;

			const sorted = [...numericValues].sort((a, b) => a - b);
			const count = numericValues.length;
			const sum = numericValues.reduce((a, b) => a + b, 0);
			const mean = sum / count;

			stats.push({
				column: h,
				count: count,
				mean: mean.toFixed(2),
				min: sorted[0],
				"25%": sorted[Math.floor(count * 0.25)],
				"50%": sorted[Math.floor(count * 0.5)],
				"75%": sorted[Math.floor(count * 0.75)],
				max: sorted[count - 1],
			});
		}

		if (stats.length === 0) {
			console.log("No numeric columns found to describe.");
		} else {
			console.table(stats);
		}
	}

	/**
	 * RENAME: Cambia los nombres de las columnas sin tocar los datos.
	 */
	rename(mapping) {
		const newColumns = {};
		const newHeaders = this.headers.map((h) => {
			const newName = mapping[h] || h;
			newColumns[newName] = this.columns[h];
			return newName;
		});

		return new DataFrame({
			...this,
			columns: newColumns,
			headers: newHeaders,
			colMap: Object.fromEntries(newHeaders.map((h, i) => [h, i])),
		});
	}

	/**
	 * HEAD: Retorna un nuevo DataFrame con las primeras N filas.
	 */
	head(n = 5) {
		const limit = Math.min(n, this.rowCount);
		const newColumns = {};

		for (const h of this.headers) {
			newColumns[h] = this.columns[h].slice(0, limit);
		}

		return new DataFrame({ ...this, columns: newColumns, rowCount: limit });
	}

	/**
	 * TAIL: Retorna un nuevo DataFrame con las últimas N filas.
	 */
	tail(n = 5) {
		const start = Math.max(0, this.rowCount - n);
		const newColumns = {};

		for (const h of this.headers) {
			newColumns[h] = this.columns[h].slice(start, this.rowCount);
		}

		return new DataFrame({
			...this,
			columns: newColumns,
			rowCount: this.rowCount - start,
		});
	}
	/**
	 * UNIQUE: Retorna los valores únicos de una columna.
	 */
	unique(columnName) {
		return [...new Set(this.columns[columnName])];
	}

	/**
	 * NUNIQUE: Conteo rápido de valores únicos.
	 */
	nunique(columnName) {
		return new Set(this.columns[columnName]).size;
	}

	/**
	 * VALUE_COUNTS: Frecuencia de valores ordenada de mayor a menor.
	 */
	value_counts(columnName) {
		const counts = {};
		const col = this.columns[columnName];

		for (let i = 0; i < this.rowCount; i++) {
			const val = col[i];
			counts[val] = (counts[val] || 0) + 1;
		}

		return Object.entries(counts)
			.sort((a, b) => b[1] - a[1])
			.map(([value, count]) => ({ value, count }));
	}

	/**
	 * DROPNA: Elimina filas que contengan null, undefined o NaN.
	 */
	dropNA() {
		const indices = [];
		for (let i = 0; i < this.rowCount; i++) {
			let hasNull = false;
			for (const h of this.headers) {
				const val = this.columns[h][i];
				if (
					val === null ||
					val === undefined ||
					(typeof val === "number" && isNaN(val))
				) {
					hasNull = true;
					break;
				}
			}
			if (!hasNull) indices.push(i);
		}

		// Reutilizamos la lógica de reconstrucción de columnas (puedes extraerla a un método privado)
		return this._rebuildFromIndices(indices);
	}

	/**
	 * FILLNA: Remplaza valores nulos por uno específico.
	 */
	fillna(value) {
		for (const h of this.headers) {
			const col = this.columns[h];
			for (let i = 0; i < this.rowCount; i++) {
				if (
					col[i] === null ||
					col[i] === undefined ||
					(typeof col[i] === "number" && isNaN(col[i]))
				) {
					col[i] = value;
				}
			}
		}
		return this; // Modifica in-place para ahorrar memoria en limpieza
	}

	/**
	 * Método auxiliar para reconstruir el DF basado en un mapa de índices
	 */
	_rebuildFromIndices(indices) {
		const newColumns = {};
		for (const h of this.headers) {
			const oldCol = this.columns[h];
			const isTyped = oldCol instanceof Float64Array;
			const newCol = isTyped
				? new Float64Array(indices.length)
				: new Array(indices.length);

			for (let j = 0; j < indices.length; j++) {
				newCol[j] = oldCol[indices[j]];
			}
			newColumns[h] = newCol;
		}
		return new DataFrame({
			...this,
			columns: newColumns,
			rowCount: indices.length,
		});
	}
	/**
	 * SELECT: Filtra columnas para liberar RAM.
	 * Vital para archivos gigantes (118GB).
	 */
	select(columnNames) {
		const newColumns = {};
		for (const name of columnNames) {
			if (this.columns[name]) {
				newColumns[name] = this.columns[name];
			}
		}
		return new DataFrame({
			...this,
			columns: newColumns,
			headers: columnNames,
			rowCount: this.rowCount,
		});
	}

	/**
	 * STR_CONTAINS: Retorna un nuevo DF con filas que contienen el patrón.
	 */
	str_contains(columnName, pattern) {
		const regex = new RegExp(pattern, "i");
		const indices = [];
		const col = this.columns[columnName];

		for (let i = 0; i < this.rowCount; i++) {
			if (col[i] && regex.test(col[i])) {
				indices.push(i);
			}
		}
		return this._rebuildFromIndices(indices);
	}

	/**
	 * CAST: Fuerza el cambio de tipo de una columna.
	 */
	cast(columnName, type) {
		const oldCol = this.columns[columnName];
		let newCol;

		if (type === "float" || type === "int") {
			newCol = new Float64Array(this.rowCount);
			for (let i = 0; i < this.rowCount; i++) {
				newCol[i] = parseFloat(oldCol[i]) || 0;
			}
		} else if (type === "string") {
			newCol = new Array(this.rowCount);
			for (let i = 0; i < this.rowCount; i++) {
				newCol[i] = String(oldCol[i]);
			}
		}

		this.columns[columnName] = newCol;
		return this;
	}

	/**
	 * CUMSUM: Suma acumulada de una columna.
	 */
	cumsum(columnName) {
		const col = this.columns[columnName];
		const newCol = new Float64Array(this.rowCount);
		let acc = 0;

		for (let i = 0; i < this.rowCount; i++) {
			acc += parseFloat(col[i]) || 0;
			newCol[i] = acc;
		}

		const newName = `${columnName}_cumsum`;
		this.columns[newName] = newCol;
		if (!this.headers.includes(newName)) this.headers.push(newName);

		return this;
	}
	/**
	 * JOIN: Une dos DataFrames por una columna común.
	 * @param {DataFrame} other - El otro DataFrame.
	 * @param {string} on - La columna llave.
	 * @param {string} how - 'inner' (solo coincidencias) o 'left' (todo el de la izquierda).
	 */
	join(other, on, how = "inner") {
		const leftCol = this.columns[on];
		const rightCol = other.columns[on];

		// 1. Fase de Hash: Mapeamos los índices del DataFrame derecho
		const rightMap = new Map();
		for (let i = 0; i < other.rowCount; i++) {
			const val = rightCol[i];
			if (!rightMap.has(val)) rightMap.set(val, []);
			rightMap.get(val).push(i);
		}

		const joinedRows = [];
		const rightHeaders = other.headers.filter((h) => h !== on);

		// 2. Fase de Probe: Recorremos el DataFrame izquierdo
		for (let i = 0; i < this.rowCount; i++) {
			const leftVal = leftCol[i];
			const matches = rightMap.get(leftVal);

			if (matches) {
				for (const rightIdx of matches) {
					const newRow = {};
					// Copiar datos de la izquierda
					this.headers.forEach((h) => (newRow[h] = this.columns[h][i]));
					// Copiar datos de la derecha (evitando duplicar la llave)
					rightHeaders.forEach((h) => (newRow[h] = other.columns[h][rightIdx]));
					joinedRows.push(newRow);
				}
			} else if (how === "left") {
				const newRow = {};
				this.headers.forEach((h) => (newRow[h] = this.columns[h][i]));
				rightHeaders.forEach((h) => (newRow[h] = null));
				joinedRows.push(newRow);
			}
		}

		return DataFrame.fromObjects(joinedRows);
	}
	col(name) {
		if (!this.columns[name]) throw new Error(`Column ${name} not found`);

		// Retornamos una instancia de Column vinculada a los datos reales
		return new Column(name, this.columns[name], this);
	}
}
