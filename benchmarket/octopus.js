import {Octopus} from '../src/index.js';

async function main() {
    console.log("🐙 NYC YELLOW TAXI - OCTOPUS ANALYTICS");
    console.log("=======================================\n");

    const startLoad = Date.now();
    const ds = await Octopus.read('./data/yellow_tripdata_2019-03.csv', { 
        indexerCapacity: 20_000_000,
        workers: 8 
    });

    const count = ds.length || ds.rowCount;
    const loadTime = (Date.now() - startLoad) / 1000;
    console.log(`✅ Cargado: ${count.toLocaleString()} viajes en ${loadTime.toFixed(2)}s`);

    // --- FEATURE ENGINEERING ---
    const startFE = Date.now();
    const c = ds.columns;

    // Aseguramos que existan los arrays de salida
    c.pickup_hour = new Int8Array(count);
    c.trip_duration_min = new Float64Array(count);
    c.tip_percentage = new Float64Array(count);
    c.profit_per_mile = new Float64Array(count);
    c.avg_speed_mph = new Float64Array(count);

    for (let i = 0; i < count; i++) {
        const pickup = c.tpep_pickup_datetime[i];
        const dropoff = c.tpep_dropoff_datetime[i];
        const dist = c.trip_distance[i];
        const fare = c.fare_amount[i];
        const tip = c.tip_amount[i];
        const tolls = c.tolls_amount[i] || 0;

        // 1. Duración y Hora
        const duration = (dropoff - pickup) / 60000;
        c.trip_duration_min[i] = duration > 0 ? duration : 0.01;
        c.pickup_hour[i] = new Date(pickup).getHours();

        // 2. Velocidad
        c.avg_speed_mph[i] = dist / (c.trip_duration_min[i] / 60);

        // 3. Propina %
        c.tip_percentage[i] = fare > 0 ? (tip / fare) * 100 : 0;

        // 4. Rentabilidad
        c.profit_per_mile[i] = dist > 0 ? (fare + tip + tolls) / dist : 0;
    }
    console.log(`🔧 Feature Engineering: ${Date.now() - startFE}ms`);

    // --- QUERIES (Usando la función de agregación ds_groupby_agg) ---
    
    // QUERY 1: Propinas por hora
    const start1 = Date.now();
    const q1 = ds_groupby_agg(ds, 'pickup_hour', 'tip_percentage');
    console.log(`⏱️ Q1 (Propinas/Hora): ${Date.now() - start1}ms`);

    // QUERY 2: Zonas Rentables
    const start2 = Date.now();
    const q2 = ds_groupby_agg(ds, 'PULocationID', 'profit_per_mile');
    console.log(`⏱️ Q2 (Rentabilidad/Zona): ${Date.now() - start2}ms`);

    // QUERY 5: Tráfico (Velocidad/Hora)
    const start5 = Date.now();
    const q5 = ds_groupby_agg(ds, 'pickup_hour', 'avg_speed_mph');
    console.log(`⏱️ Q5 (Velocidad/Hora): ${Date.now() - start5}ms`);

    console.log(`\n⏱️ TIEMPO TOTAL OCTOPUS: ${(Date.now() - startLoad) / 1000}s`);
}

/**
 * Agregación ultra-rápida estilo Pandas
 */
function ds_groupby_agg(dataset, groupCol, targetCol) {
    const groups = new Map();
    const gData = dataset.columns[groupCol];
    const tData = dataset.columns[targetCol];
    const len = dataset.length || dataset.rowCount;

    for (let i = 0; i < len; i++) {
        const key = gData[i];
        const val = tData[i];
        if (!groups.has(key)) {
            groups.set(key, { sum: 0, count: 0 });
        }
        const g = groups.get(key);
        g.sum += val;
        g.count++;
    }
    return groups;
}

main().catch(console.error);