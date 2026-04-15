/**
 * OneHotEncoder Nitro - Optimizado para Cervid (TypedArrays + IDs)
 */
export class OneHotEncoder {
	constructor(options = {}) {
		this.options = {
			dropFirst: false,
			...options,
		};
		this.categories = new Map();
		this.isFitted = false;
	}
	fit(df, column) {
		const colData = df.columns[column];
		const uniqueIds = new Set();

		// Usamos un loop simple para máxima velocidad
		for (let i = 0; i < df.rowCount; i++) {
			const id = colData[i];
			// Solo IDs válidos (Cervid usa 0, 1, 2... para strings)
			if (id !== undefined && id !== null) {
				uniqueIds.add(id);
			}
		}

		this.categories.set(
			column,
			Array.from(uniqueIds).sort((a, b) => a - b),
		);
		this.isFitted = true;
		return this;
	}

	fitTransform(df, column) {
		if (!this.isFitted) this.fit(df, column);

		const categories = this.categories.get(column);
		const colData = df.columns[column];
		const rowCount = df.rowCount;

		// Forzamos que cada columna tenga EXACTAMENTE el tamaño del DataFrame
		const resultCols = categories.map(() => new Uint8Array(rowCount));

		for (let i = 0; i < rowCount; i++) {
			const currentId = colData[i];
			for (let j = 0; j < categories.length; j++) {
				if (currentId === categories[j]) {
					resultCols[j][i] = 1;
					break;
				}
			}
		}

		return resultCols;
	}
}
