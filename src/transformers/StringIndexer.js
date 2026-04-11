export class StringIndexer {
	constructor(options = {}) {
		this.options = { handleUnknown: "keep", ...options };
		this.maps = {};
		this.labels = {};
		this.decoder = new TextDecoder();
	}

	fitTransform(df, input) {
		// Normalizar nombre de columna
		const colName = Array.isArray(input) ? input[0] : input;

		const distinctValues = new Set();
		const map = new Map();
		const labels = [];

		// 1. Scan de bytes usando los offsets del DF
		for (let i = 0; i < df.rowCount; i++) {
			const val = this._extract(df, colName, i);
			distinctValues.add(val);
		}

		// 2. Crear Diccionario
		Array.from(distinctValues)
			.sort()
			.forEach((val, idx) => {
				map.set(val, idx);
				labels.push(val);
			});

		// 3. Guardar bajo el nombre de la columna (Evita el error de undefined)
		this.maps[colName] = map;
		this.labels[colName] = labels;

		// 4. Transformar
		const result = new Float64Array(df.rowCount);
		for (let i = 0; i < df.rowCount; i++) {
			const val = this._extract(df, colName, i);
			result[i] = map.get(val) ?? labels.length;
		}
		return result;
	}

	_extract(df, colName, rowIdx) {
		const colIdx = df.colMap[colName];
		const offIdx = (rowIdx * df.numCols + colIdx) * 2;
		const start = df.offsets[offIdx];
		const end = df.offsets[offIdx + 1];
		return this.decoder.decode(df.originalBuffer.subarray(start, end)).trim();
	}

	getLabels(colName) {
		const labels = this.labels[colName];
		if (!labels)
			throw new Error(`StringIndexer: No hay etiquetas para "${colName}"`);
		return labels;
	}
	// Añade estos métodos a tu clase StringIndexer
	getIndex(colName, label) {
		// Si solo se pasó un argumento, asumimos que es el label y buscamos en el primer mapa
		if (label === undefined) {
			const firstCol = Object.keys(this.maps)[0];
			return this.maps[firstCol].get(colName) ?? -1;
		}
		const map = this.maps[colName];
		return map ? (map.get(label) ?? -1) : -1;
	}

	getLabels(colName) {
		const target = colName || Object.keys(this.labels)[0];
		return this.labels[target] || [];
	}
}
