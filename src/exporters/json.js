import fs from 'fs';

export class JSONExporter {
  constructor(df, options = {}) {
    this.df = df;
    this.options = {
      pretty: false,
      ndjson: false,  // Newline Delimited JSON
      ...options
    };
  }

  async export(outputPath) {
    if (this.options.ndjson) {
      return await this._exportNDJSON(outputPath);
    }
    return await this._exportJSON(outputPath);
  }

  async _exportNDJSON(outputPath) {
    const stream = fs.createWriteStream(outputPath, { encoding: 'utf8' });
    const columns = Object.keys(this.df.columns);
    
    for (let i = 0; i < this.df.rowCount; i++) {
      const row = {};
      columns.forEach(col => {
        row[col] = this.df.columns[col][i];
      });
      stream.write(JSON.stringify(row) + '\n');
      
      if (i % 100000 === 0 && i > 0) {
        await new Promise(resolve => setImmediate(resolve));
      }
    }
    
    stream.end();
    console.log(`✅ NDJSON exportado: ${outputPath} (${this.df.rowCount.toLocaleString()} filas)`);
    
    return { path: outputPath, rows: this.df.rowCount, format: 'ndjson' };
  }

  async _exportJSON(outputPath) {
    const data = this.df.toArray();
    const json = this.options.pretty 
      ? JSON.stringify(data, null, 2)
      : JSON.stringify(data);
    
    await fs.promises.writeFile(outputPath, json, 'utf8');
    console.log(`✅ JSON exportado: ${outputPath} (${this.df.rowCount.toLocaleString()} filas)`);
    
    return { path: outputPath, rows: this.df.rowCount, format: 'json' };
  }
}