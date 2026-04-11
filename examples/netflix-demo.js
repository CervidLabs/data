import { Octopus, StringIndexer, OneHotEncoder, DataFrame } from '../src/index.js';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataPath = path.join(__dirname, '..', 'data', 'netflix_titles.csv');

async function main() {
  console.log('🐙 NYC NETFLIX TITLES - NITRO TRANSFORMERS');
  console.log('===========================================\n');

  // 1. CARGA NITRO
  console.log('🚀 Cargando dataset con Octopus Nitro...');
  const df = await Octopus.read(dataPath);
  console.log(`✅ Cargado: ${df.rowCount.toLocaleString()} registros en memoria compartida.\n`);

  // 2. LIMPIEZA NITRO (Usando IDs numéricos para nulos)
  console.log('🧹 Limpiando nulos...');
  const UNKNOWN_HASH = -1; 
  const columnsToClean = ['country', 'director', 'cast', 'rating', 'type'];

  for (const col of columnsToClean) {
    const colData = df.columns[col];
    let nullCount = 0;
    for (let i = 0; i < df.rowCount; i++) {
      // Si el valor es 0, NaN o nulo (depende del hash del worker)
      if (!colData[i] || isNaN(colData[i])) {
        colData[i] = UNKNOWN_HASH;
        nullCount++;
      }
    }
    console.log(`   - ${col}: ${nullCount} nulos marcados como Unknown`);
  }

  // 3. STRING INDEXER (El Diccionario)
  console.log('\n🏷️ StringIndexer: Creando mapas de categorías...');
  const indexer = new StringIndexer({ handleUnknown: 'keep' });
  
  // IMPORTANTE: Indexamos country para poder traducirlo después
  const dfIndexed = indexer.fitTransform(df, ['type', 'rating', 'country']);

  const typeLabels = indexer.getLabels('type');
  console.log(`✅ Categorías detectadas en 'type': ${typeLabels.join(', ')}`);

  // 4. ONE-HOT ENCODER
  console.log('\n🔥 OneHotEncoder: Generando columnas binarias...');
  const encoder = new OneHotEncoder();
  const dfEncoded = encoder.fitTransform(dfIndexed, ['type']);
  
  const featureNames = encoder.getFeatureNames();
  featureNames.forEach(feat => {
    const count = dfEncoded.columns[feat].filter(v => v === 1).length;
    console.log(`   - ${feat}: ${count} registros`);
  });

  // 5. EXPORTACIÓN
  console.log('\n💾 Exportando resultados...');
  const outputDir = path.join(__dirname, '..', 'data');
  
  // Creamos un subset para exportar
  const exportDf = new DataFrame({
    columns: {
      title: df.columns.title,
      type: df.columns.type,
      type_indexed: dfIndexed.columns.type_indexed,
      country: df.columns.country,
      rating_indexed: dfIndexed.columns.rating_indexed
    },
    rowCount: df.rowCount
  });

  await exportDf.toCSV(path.join(outputDir, 'netflix_processed.csv'));
  await exportDf.toJSON(path.join(outputDir, 'netflix_processed.json'));

  // 9. RESUMEN FINAL (Sincronización de IDs)
// 9. RESUMEN FINAL corregido
console.log('\n📊 RESUMEN FINAL:');

// El indexer ahora calcula el hash de "Movie" y te da su ID (0, 1, etc)
const movieIdx = indexer.getIndex('type', 'Movie');
const tvShowIdx = indexer.getIndex('type', 'TV Show');

const typeDataIndexed = dfIndexed.columns['type_indexed'];
let movieCount = 0;
let tvShowCount = 0;

for (let i = 0; i < df.rowCount; i++) {
  const val = typeDataIndexed[i];
  if (val === movieIdx) movieCount++;
  else if (val === tvShowIdx) tvShowCount++;
}

// Para los ratings reales:
// En Netflix los ratings son pocos (14). Si te salen 6015, es que 
// estás indexando una columna que NO es la de ratings (quizás la de IDs).
const ratingCategoriesCount = indexer.getLabels('rating').length;

console.log(`   Tipos: Movie (${movieCount}), TV Show (${tvShowCount})`);
console.log(`   Categorías de Rating: ${ratingCategoriesCount}`);
  console.log('\n🚀 Transformers demo completado con éxito!');
}

main().catch(err => {
  console.error('❌ Error en el demo:', err);
});