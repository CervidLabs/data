import { DataFrame } from "../core/DataFrame.js";

/**
 * MinMaxScaler - Normalización a rango [0, 1]
 */
export class MinMaxScaler {
	constructor(options = {}) {
		this.options = {
			featureRange: [0, 1],
			...options,
		};
		this.min_ = new Map();
		this.max_ = new Map();
		this.isFitted = false;
	}

	fit(df, columns) {
		const cols = Array.isArray(columns) ? columns : [columns];
		const [rangeMin, rangeMax] = this.options.featureRange;

		for (const col of cols) {
			const values = df.columns[col].filter(
				(v) => typeof v === "number" && !isNaN(v),
			);
			const min = Math.min(...values);
			const max = Math.max(...values);

			this.min_.set(col, min);
			this.max_.set(col, max === min ? 1 : max - min);
		}

		this.isFitted = true;
		return this;
	}

	transform(df) {
		if (!this.isFitted) {
			throw new Error("MinMaxScaler must be fitted first");
		}

		const [rangeMin, rangeMax] = this.options.featureRange;
		const newColumns = { ...df.columns };

		for (const [col, min] of this.min_.entries()) {
			const range = this.max_.get(col);
			const scaledCol = `${col}_normalized`;

			newColumns[scaledCol] = [];
			for (let i = 0; i < df.rowCount; i++) {
				const value = df.columns[col][i];
				if (typeof value === "number" && !isNaN(value)) {
					const normalized = (value - min) / range;
					const scaled = normalized * (rangeMax - rangeMin) + rangeMin;
					newColumns[scaledCol].push(scaled);
				} else {
					newColumns[scaledCol].push(value);
				}
			}
		}

		return new DataFrame({
			columns: newColumns,
			rowCount: df.rowCount,
		});
	}

	fitTransform(df, columns) {
		return this.fit(df, columns).transform(df);
	}
}
