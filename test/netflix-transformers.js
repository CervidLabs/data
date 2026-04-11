import { Octopus, StringIndexer, OneHotEncoder, StandardScaler, Pipeline } from '../src/index.js';
import path from 'path';
import { fileURLToPath } from 'url';
import { DataFrame } from '../src/core/DataFrame.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataPath = path.join(__dirname, '..', 'data', 'netflix_titles.csv');

async function main() {
  console.log('🎬 NETFLIX TITLES - TRANSFORMERS DEMO');
  console.log('=====================================\n');

  // 1. Cargar datos
  console.log('📂 Cargando Netflix dataset...');
  const df = await Octopus.read(dataPath);
  console.log(`   ✅ Cargado: ${df.rowCount.toLocaleString()} películas/series`);
  console.log(`   📊 Columnas: ${Object.keys(df.columns).join(', ')}\n`);

  // 2. Limpieza básica
  console.log('🧹 Limpiando datos nulos...');
  
  // Rellenar nulos en columnas clave
  const columnsToClean = ['country', 'director', 'cast', 'rating'];
  for (const col of columnsToClean) {
    let nullCount = 0;
    for (let i = 0; i < df.rowCount; i++) {
      if (!df.columns[col]?.[i] || df.columns[col][i] === '') {
        df.columns[col][i] = 'Unknown';
        nullCount++;
      }
    }
    console.log(`   ${col}: ${nullCount} nulos rellenados con 'Unknown'`);
  }
  
  // Rellenar rating
  for (let i = 0; i < df.rowCount; i++) {
    if (!df.columns.rating[i] || df.columns.rating[i] === '') {
      df.columns.rating[i] = 'Not Rated';
    }
  }
  
  console.log('   ✅ Limpieza completada\n');

  // 3. StringIndexer - Convertir a índices
  console.log('🏷️ StringIndexer: convirtiendo categorías...');
  
  const indexer = new StringIndexer({ handleUnknown: 'keep' });
  const dfWithIndexes = indexer.fitTransform(df, ['type', 'rating']);
  
  console.log('   Columnas indexadas:');
  console.log(`   - type_indexed: Movie=0, TV Show=1`);
  console.log(`   - rating_indexed: ${indexer.getLabels('rating').slice(0, 5).join(', ')}... (${indexer.getLabels('rating').length} total)`);
  
  // Mostrar muestra
  console.log('\n   Muestra:');
  for (let i = 0; i < 5; i++) {
    console.log(`   ${df.columns.title[i]} | ${df.columns.type[i]} → ${dfWithIndexes.columns.type_indexed[i]} | rating: ${df.columns.rating[i]} → ${dfWithIndexes.columns.rating_indexed[i]}`);
  }

  // 4. OneHotEncoder - Para tipo (Movie/TV Show)
  console.log('\n🔥 OneHotEncoder: creando columnas binarias para "type"...');
  
  const encoder = new OneHotEncoder({ dropFirst: false });
  const dfEncoded = encoder.fitTransform(df, ['type']);
  
  console.log('   Columnas creadas:');
  const featureNames = encoder.getFeatureNames();
  for (const feat of featureNames) {
    if (dfEncoded.columns[feat]) {
      const ones = dfEncoded.columns[feat].filter(v => v === 1).length;
      const percentage = ((ones / df.rowCount) * 100).toFixed(1);
      console.log(`   - ${feat}: ${ones.toLocaleString()} registros (${percentage}%)`);
    } else {
      console.log(`   - ${feat}: [ERROR - columna no encontrada]`);
    }
  }

  // 5. Pipeline con StandardScaler (para release_year)
  console.log('\n🚀 Pipeline: escalando release_year...');
  
  // Verificar que release_year existe y es numérico
  let yearScaled = false;
  if (df.columns.release_year) {
    // Crear versión escalada manualmente
    const years = [];
    for (let i = 0; i < df.rowCount; i++) {
      const year = df.columns.release_year[i];
      if (typeof year === 'number' && !isNaN(year)) {
        years.push(year);
      }
    }
    
    const minYear = Math.min(...years);
    const maxYear = Math.max(...years);
    const range = maxYear - minYear;
    
    df.columns.release_year_scaled = [];
    for (let i = 0; i < df.rowCount; i++) {
      const year = df.columns.release_year[i];
      if (typeof year === 'number' && !isNaN(year)) {
        const scaled = range === 0 ? 0 : (year - minYear) / range;
        df.columns.release_year_scaled.push(scaled);
      } else {
        df.columns.release_year_scaled.push(0);
      }
    }
    
    console.log(`   release_year: min=${minYear}, max=${maxYear}, range=${range}`);
    console.log(`   Nueva columna: release_year_scaled (0-1 normalizado)`);
    yearScaled = true;
  } else {
    console.log('   ⚠️ release_year no encontrada, saltando escalado');
  }

  // 6. Análisis con datos transformados
  console.log('\n📊 Análisis: Películas por año (normalizado)');
  
  const moviesByYear = {};
  for (let i = 0; i < df.rowCount; i++) {
    const type = df.columns.type[i];
    const year = df.columns.release_year[i];
    
    if (type === 'Movie' && typeof year === 'number' && year >= 2010) {
      moviesByYear[year] = (moviesByYear[year] || 0) + 1;
    }
  }
  
  const sortedYears = Object.entries(moviesByYear)
    .sort((a, b) => parseInt(b[0]) - parseInt(a[0]))
    .slice(0, 5);
  
  console.log('   Top 5 años recientes con más películas:');
  for (const [year, count] of sortedYears) {
    console.log(`   ${year}: ${count} películas`);
  }

  // 7. Distribución de ratings
  console.log('\n⭐ Distribución de ratings:');
  
  const ratingCount = {};
  for (let i = 0; i < df.rowCount; i++) {
    const rating = df.columns.rating[i];
    if (rating && rating !== 'Not Rated') {
      ratingCount[rating] = (ratingCount[rating] || 0) + 1;
    }
  }
  
  Object.entries(ratingCount)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .forEach(([rating, count]) => {
      const pct = ((count / df.rowCount) * 100).toFixed(1);
      console.log(`   ${rating}: ${count.toLocaleString()} (${pct}%)`);
    });

  // 8. Exportar resultados
  console.log('\n💾 Exportando datos procesados...');
  
  const outputDir = path.join(__dirname, '..', 'data');
  
  // Crear DataFrame con columnas seleccionadas para exportar
  const exportColumns = {
    title: df.columns.title,
    type: df.columns.type,
    type_indexed: dfWithIndexes.columns.type_indexed,
    rating: df.columns.rating,
    rating_indexed: dfWithIndexes.columns.rating_indexed,
    release_year: df.columns.release_year
  };
  
  if (yearScaled) {
    exportColumns.release_year_scaled = df.columns.release_year_scaled;
  }
  
  // Agregar columnas one-hot si existen
  for (const feat of featureNames) {
    if (dfEncoded.columns[feat]) {
      exportColumns[feat] = dfEncoded.columns[feat];
    }
  }
  
  const exportDf = new DataFrame({ columns: exportColumns, rowCount: df.rowCount });
  
  await exportDf.toCSV(path.join(outputDir, 'netflix_processed.csv'));
  await exportDf.toJSON(path.join(outputDir, 'netflix_processed.json'), { pretty: true });
  
  console.log('   ✅ Exportado a:');
  console.log('      - data/netflix_processed.csv');
  console.log('      - data/netflix_processed.json');

  // 9. Estadísticas finales
  console.log('\n📈 RESUMEN FINAL:');
  console.log(`   Total registros: ${df.rowCount.toLocaleString()}`);
  console.log(`   Tipos: Movie (${ratingCount.Movie || df.columns.type.filter(t => t === 'Movie').length}), TV Show (${df.columns.type.filter(t => t === 'TV Show').length})`);
  console.log(`   Ratings únicos: ${Object.keys(ratingCount).length}`);
  console.log(`   Países (primeros 5): ${df.columns.country.slice(0, 5).join(', ')}...`);
  
  console.log('\n✅ Transformers demo completado!');
}

main().catch(console.error);