import fs from "fs";
import path from "path";

export class CSVExporter {
	constructor(df, options = {}) {
		this.df = df;
		this.options = {
			delimiter: ",",
			header: true,
			encoding: "utf8",
			...options,
		};
	}

	async export(outputPath) {
		const stream = fs.createWriteStream(outputPath, {
			encoding: this.options.encoding,
		});
		const columns = Object.keys(this.df.columns);

		// Escribir header
		if (this.options.header) {
			stream.write(columns.join(this.options.delimiter) + "\n");
		}

		// Escribir filas
		for (let i = 0; i < this.df.rowCount; i++) {
			const row = columns.map((col) => {
				let value = this.df.columns[col][i];
				if (value === null || value === undefined) return "";
				if (
					typeof value === "string" &&
					(value.includes(this.options.delimiter) || value.includes('"'))
				) {
					value = `"${value.replace(/"/g, '""')}"`;
				}
				return value;
			});
			stream.write(row.join(this.options.delimiter) + "\n");

			// Liberar memoria cada 100k filas
			if (i % 100000 === 0 && i > 0) {
				await new Promise((resolve) => setImmediate(resolve));
			}
		}

		stream.end();
		console.log(
			`CSV exported: ${outputPath} (${this.df.rowCount.toLocaleString()} filas)`,
		);

		return { path: outputPath, rows: this.df.rowCount };
	}
}
