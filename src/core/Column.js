export class Column {
	constructor(name, data, parentDf) {
		this.name = name;
		this.data = data;
		this.length = data.length;
		this.parentDf = parentDf;
	}

	add(value) {
		const isCol = value instanceof Column;
		for (let i = 0; i < this.length; i++) {
			this.data[i] += isCol ? value.data[i] : value;
		}
		return this;
	}

	sub(value) {
		const isCol = value instanceof Column;
		for (let i = 0; i < this.length; i++) {
			this.data[i] -= isCol ? value.data[i] : value;
		}
		return this;
	}

	mul(value) {
		const isCol = value instanceof Column;
		for (let i = 0; i < this.length; i++) {
			this.data[i] *= isCol ? value.data[i] : value;
		}
		return this;
	}

	div(value) {
		const isCol = value instanceof Column;
		for (let i = 0; i < this.length; i++) {
			const divisor = isCol ? value.data[i] : value;
			this.data[i] /= divisor !== 0 ? divisor : 1;
		}
		return this;
	}

	to_datetime() {
		for (let i = 0; i < this.length; i++) {
			this.data[i] = new Date(this.data[i]).getTime();
		}
		return this;
	}
	/**
	 * Extrae la hora (0-23) de un timestamp milisegundos.
	 * Crea una nueva columna en el DataFrame padre.
	 */
	extract_hour(offsetSeconds = 32400) {
		const hours = new Float64Array(this.length);
		for (let i = 0; i < this.length; i++) {
			// Convertimos ms a segundos, restamos offset, volvemos a horas
			const totalSeconds = Math.floor(this.data[i] / 1000) - offsetSeconds;
			const secondsInDay = ((totalSeconds % 86400) + 86400) % 86400;
			hours[i] = Math.floor(secondsInDay / 3600);
		}
		const newName = `${this.name}_hour`;
		this.parentDf.columns[newName] = hours;
		return this;
	}
}
