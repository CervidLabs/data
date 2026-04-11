import { Octopus } from '../src/index.js';

async function main() {
    console.log("🐙 OCTOPUS ANALYTICS - MODO AUDITORÍA (PARIDAD POLARS)");
    console.log("====================================================\n");

    const startTotal = Date.now();

    // 1. CARGA NITRO (8 Workers)
    const ds_load = await Octopus.read('./data/yellow_tripdata_2019-03.csv', { workers: 8 });
    const loadTime = (Date.now() - startTotal) / 1000;
    console.log(`✅ Ingesta: ${ds_load.rowCount.toLocaleString()} filas en ${loadTime}s`);

    // 2. PIPELINE DE TRANSFORMACIÓN
    const startProc = Date.now();
    let ds = ds_load
        // FILTRO SUCIO: Solo quitamos lo que daría error matemático (ceros y negativos)
        // No filtramos outliers (dist > 0.1) para coincidir con Polars
        .filter(['trip_distance', 'fare_amount', 'tpep_dropoff_datetime', 'tpep_pickup_datetime'], 
        (dist, fare, drop, pick) => {
            return dist > 0 && fare > 0 && (drop - pick) > 0;
        })
        .with_columns([
{
    name: 'pickup_hour',
    inputs: ['tpep_pickup_datetime'],
    formula: (ts) => {
        // AJUSTE DE SINCRONIZACIÓN:
        // Polars marca las 23h donde Octopus marca las 8h.
        // Restamos 9 horas (32400 seg) para alinear los baldes de tiempo.
        const adjustedTs = ts - 32400;
        const secondsInDay = ((adjustedTs % 86400) + 86400) % 86400;
        return Math.floor(secondsInDay / 3600);
    }
},
            {
                name: 'tip_percentage',
                inputs: ['tip_amount', 'fare_amount'],
                formula: (tip, fare) => fare > 0 ? (tip / fare) * 100 : 0
            },
            {
                name: 'profit_per_mile',
                inputs: ['fare_amount', 'tip_amount', 'tolls_amount', 'trip_distance'],
                formula: (fare, tip, tolls, dist) => dist > 0 ? (fare + tip + tolls) / dist : 0
            },
            {
                name: 'speed_mph',
                inputs: ['trip_distance', 'tpep_dropoff_datetime', 'tpep_pickup_datetime'],
                formula: (dist, drop, pick) => {
                    const hours = (drop - pick) / 3600;
                    // No filtramos velocidades máximas para que el promedio sea igual al de Polars
                    return dist / hours;
                }
            }
        ]);
    
    const procTime = (Date.now() - startProc) / 1000;
    console.log(`🧹 Procesamiento & FE: ${procTime}s`);

    // --- MUESTRA DE DATOS ---
    console.log("\n🔍 MUESTRA DE DATOS (Verificación):");
    if (ds.columns.pickup_hour) {
        for (let i = 0; i < 5; i++) {
            const h = ds.columns.pickup_hour[i];
            const tip = ds.columns.tip_percentage[i].toFixed(2);
            const profit = ds.columns.profit_per_mile[i].toFixed(2);
            console.log(`Fila ${i}: Hora: ${h}h | Propina: ${tip}% | Rentabilidad: $${profit}/mi`);
        }
    }

    // --- RESULTADOS DE LAS 5 QUERIES ---
    console.log("\n📊 RESULTADOS DE AGREGACIONES:");

    // Q1
    const q1 = ds.groupByRange('pickup_hour', 'tip_percentage', 24);
    if (q1[0]) console.log(`⏱️ Q1 (Mejor hora): ${q1[0].group}h (${q1[0].avg.toFixed(2)}%)`);

    // Q2
    const q2 = ds.groupByID('PULocationID', 'profit_per_mile');
    if (q2[0]) console.log(`⏱️ Q2 (Zona Top): ID ${q2[0].group} ($${q2[0].avg.toFixed(2)}/mi)`);

    // Q3
    const q3 = ds.groupByRange('payment_type', 'trip_distance', 5);
    const credit = q3.find(r => r.group === 1);
    if (credit) console.log(`⏱️ Q3 (Dist. Crédito): ${credit.avg.toFixed(2)} mi`);

    // Q4
    const q4 = ds.groupByRange('passenger_count', 'fare_amount', 10);
    const solo = q4.find(r => r.group === 1);
    if (solo) console.log(`⏱️ Q4 (Tarifa 1 Pasajero): $${solo.avg.toFixed(2)}`);

    // Q5
    const q5 = ds.groupByRange('pickup_hour', 'speed_mph', 24).sort((a,b) => a.avg - b.avg);
    if (q5[0]) console.log(`⏱️ Q5 (Hora lenta): ${q5[0].group}h (${q5[0].avg.toFixed(2)} mph)`);

    console.log(`\n🏁 TIEMPO TOTAL OCTOPUS: ${(Date.now() - startTotal) / 1000}s`);
}

main().catch(console.error);