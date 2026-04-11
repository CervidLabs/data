/**
 * LabelEncoder - Versión simplificada para una sola columna
 */
export class LabelEncoder {
	constructor() {
		this.classes_ = [];
		this.classToIndex = new Map();
		this.isFitted = false;
	}

	fit(values) {
		this.classes_ = [
			...new Set(values.filter((v) => v !== null && v !== undefined)),
		].sort();
		this.classes_.forEach((cls, idx) => {
			this.classToIndex.set(cls, idx);
		});
		this.isFitted = true;
		return this;
	}

	transform(values) {
		if (!this.isFitted) {
			throw new Error("LabelEncoder must be fitted first");
		}

		return values.map((v) => {
			if (v === null || v === undefined) return -1;
			return this.classToIndex.get(v) ?? -1;
		});
	}

	fitTransform(values) {
		return this.fit(values).transform(values);
	}

	inverseTransform(indices) {
		return indices.map((idx) => {
			if (idx < 0 || idx >= this.classes_.length) return null;
			return this.classes_[idx];
		});
	}
}
