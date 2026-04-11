import fs from 'fs';
import { Octopus } from '../src/index.js';

async function main() {
    console.log("🐙 OCTOPUS ML - MEGALODON 118GB LOADER");
    console.log("=======================================\n");

    const filePath = './data/All_Amazon_Review.json';
    const stats = fs.statSync(filePath);
    const fileSizeInGB = stats.size / (1024 ** 3);
    console.log(`📦 Tamaño del archivo: ${fileSizeInGB.toFixed(2)} GB`);

    // --- ESTIMACIÓN DE MEMORIA ---
    // Para 118GB de JSON, podrías tener ~500 millones de reseñas.
    // Necesitamos pre-asignar Buffers grandes o usar archivos temporales.
    const ESTIMATED_ROWS = 100_000_000; 
    const overallCol = new Float32Array(ESTIMATED_ROWS); 
    const verifiedCol = new Uint8Array(ESTIMATED_ROWS);

    const start = Date.now();
    let rowIdx = 0;
    let buffer = "";

    // Leemos con un buffer de 1MB para mantener el disco ocupado
    const stream = fs.createReadStream(filePath, { 
        encoding: 'utf8', 
        highWaterMark: 1024 * 1024 
    });

    console.log(`🚀 Procesando Stream binario...`);

    for await (const chunk of stream) {
        buffer += chunk;
        
        let startIdx = 0;
        while (true) {
            const openBrace = buffer.indexOf('{', startIdx);
            if (openBrace === -1) break;
            
            const closeBrace = buffer.indexOf('}', openBrace);
            if (closeBrace === -1) break;

            const jsonStr = buffer.substring(openBrace, closeBrace + 1);
            
            try {
                // EXTRACCIÓN ULTRA-RÁPIDA (Regex o JSON.parse)
                // Para 118GB, a veces es más rápido un Regex para sacar solo 'overall'
                const match = jsonStr.match(/"overall":\s*(\d+(\.\d+)?)/);
                const isVerified = jsonStr.includes('"verified": true');

                if (match) {
                    overallCol[rowIdx] = parseFloat(match[1]);
                    verifiedCol[rowIdx] = isVerified ? 1 : 0;
                    rowIdx++;
                }
            } catch (e) {}

            startIdx = closeBrace + 1;

            if (rowIdx % 1_000_000 === 0) {
                const elapsed = (Date.now() - start) / 1000;
                const speed = (rowIdx / elapsed).toFixed(0);
                console.log(`   ⚡ ${rowIdx.toLocaleString()} filas | Velocidad: ${speed} r/s`);
            }
        }
        // Mantener solo el resto del buffer
        buffer = buffer.substring(startIdx);
    }

    // 2. CREACIÓN DEL DATAFRAME DESDE MEMORIA CONTIGUA
    const ds = new Octopus.DataFrame({
        columns: {
            overall: overallCol.subarray(0, rowIdx),
            verified: verifiedCol.subarray(0, rowIdx)
        },
        rowCount: rowIdx
    });

    console.log(`\n✅ Carga completada.`);
    console.log(`📊 Total registros: ${ds.rowCount.toLocaleString()}`);
    console.log(`⚡ Tiempo total: ${((Date.now() - start) / 60000).toFixed(2)} minutos`);
}

main().catch(console.error);