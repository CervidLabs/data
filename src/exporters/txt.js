import fs from 'fs';

export class TXTExporter {
  constructor(df, options = {}) {
    this.df = df;
    this.options = {
      delimiter: ' | ',
      header: true,
      maxRows: 1000,  // Para TXT, solo primeras N filas
      ...options
    };
  }

  async export(outputPath) {
    const columns = Object.keys(this.df.columns);
    const maxRows = Math.min(this.options.maxRows, this.df.rowCount);
    const lines = [];
    
    // Calcular anchos de columna
    const colWidths = {};
    columns.forEach(col => {
      let maxLen = col.length;
      for (let i = 0; i < maxRows; i++) {
        const val = String(this.df.columns[col][i] ?? '');
        maxLen = Math.max(maxLen, val.length);
      }
      colWidths[col] = Math.min(maxLen, 50); // Limitar a 50 chars
    });
    
    // Header
    if (this.options.header) {
      const headerLine = columns.map(col => col.padEnd(colWidths[col])).join(this.options.delimiter);
      lines.push(headerLine);
      lines.push('-'.repeat(headerLine.length));
    }
    
    // Filas
    for (let i = 0; i < maxRows; i++) {
      const row = columns.map(col => {
        let val = String(this.df.columns[col][i] ?? '');
        if (val.length > 50) val = val.slice(0, 47) + '...';
        return val.padEnd(colWidths[col]);
      });
      lines.push(row.join(this.options.delimiter));
    }
    
    if (this.df.rowCount > maxRows) {
      lines.push(`\n... y ${(this.df.rowCount - maxRows).toLocaleString()} filas más`);
    }
    
    await fs.promises.writeFile(outputPath, lines.join('\n'), 'utf8');
    console.log(`✅ TXT exportado: ${outputPath} (${maxRows.toLocaleString()} filas mostradas)`);
    
    return { path: outputPath, rows: maxRows, totalRows: this.df.rowCount };
  }
}