import { Cervid } from "../src/index.js";

async function main() {
	console.log("Cervid ANALYTICS - AUDIT MODE");
	console.log("==============================================\n");

	const startTotal = Date.now();

	// 1. NITRO LOAD (8 Workers)
	const ds_load = await Cervid.read("./data/yellow_tripdata_2019-03.csv", {
		workers: 8,
	});
	const loadTime = (Date.now() - startTotal) / 1000;
	console.log(
		`Ingest: ${ds_load.rowCount.toLocaleString()} rows in ${loadTime}s`,
	);

	// 2. TRANSFORMATION PIPELINE
	const startProc = Date.now();
	let ds = ds_load
		// DATA CLEANING: Remove mathematical errors (zeros and negatives)
		.filter(
			[
				"trip_distance",
				"fare_amount",
				"tpep_dropoff_datetime",
				"tpep_pickup_datetime",
			],
			(dist, fare, drop, pick) => {
				return dist > 0 && fare > 0 && drop - pick > 0;
			},
		)
		.with_columns([
			{
				name: "pickup_hour",
				inputs: ["tpep_pickup_datetime"],
				formula: (ts) => {
					// Synchronization adjustment (UTC offset)
					const adjustedTs = ts - 32400;
					const secondsInDay = ((adjustedTs % 86400) + 86400) % 86400;
					return Math.floor(secondsInDay / 3600);
				},
			},
			{
				name: "tip_percentage",
				inputs: ["tip_amount", "fare_amount"],
				formula: (tip, fare) => (fare > 0 ? (tip / fare) * 100 : 0),
			},
			{
				name: "profit_per_mile",
				inputs: ["fare_amount", "tip_amount", "tolls_amount", "trip_distance"],
				formula: (fare, tip, tolls, dist) =>
					dist > 0 ? (fare + tip + tolls) / dist : 0,
			},
			{
				name: "speed_mph",
				inputs: [
					"trip_distance",
					"tpep_dropoff_datetime",
					"tpep_pickup_datetime",
				],
				formula: (dist, drop, pick) => {
					const hours = (drop - pick) / 3600;
					return dist / hours;
				},
			},
		]);

	const procTime = (Date.now() - startProc) / 1000;
	console.log(`Processing & FE: ${procTime}s`);

	// --- DATA SAMPLE ---
	console.log("\nDATA SAMPLE (Verification):");
	if (ds.columns.pickup_hour) {
		for (let i = 0; i < 5; i++) {
			const tip = ds.columns.tip_percentage[i].toFixed(2);
			const profit = ds.columns.profit_per_mile[i].toFixed(2);
			console.log(`Row ${i}: Tip: ${tip}% | Profitability: $${profit}/mi`);
		}
	}

	// --- AGGREGATION RESULTS (5 QUERIES) ---
	console.log("\nAGGREGATION RESULTS:");

	// Q1: Best hour for tips
	const q1 = ds.groupByRange("pickup_hour", "tip_percentage", 24);
	if (q1[0])
		console.log(`Q1 (Best Hour): ${q1[0].group}h (${q1[0].avg.toFixed(2)}%)`);

	// Q2: Most profitable zone
	const q2 = ds.groupByID("PULocationID", "profit_per_mile");
	if (q2[0])
		console.log(
			`Q2 (Top Zone): ID ${q2[0].group} ($${q2[0].avg.toFixed(2)}/mi)`,
		);

	// Q3: Avg distance for credit payments
	const q3 = ds.groupByRange("payment_type", "trip_distance", 6);
	const credit = q3.find((r) => r.group === 1);
	if (credit) console.log(`Q3 (Credit Dist): ${credit.avg.toFixed(2)} mi`);

	// Q4: Avg fare for single passengers
	const q4 = ds.groupByRange("passenger_count", "fare_amount", 10);
	const solo = q4.find((r) => r.group === 1);
	if (solo) console.log(`Q4 (Single Passenger Fare): $${solo.avg.toFixed(2)}`);

	// Q5: Slowest hour (Traffic)
	const q5 = ds
		.groupByRange("pickup_hour", "speed_mph", 24)
		.sort((a, b) => a.avg - b.avg);
	if (q5[0])
		console.log(
			`Q5 (Slow Hour): ${q5[0].group}h (${q5[0].avg.toFixed(2)} mph)`,
		);

	console.log(`\nCervid TOTAL TIME: ${(Date.now() - startTotal) / 1000}s`);
}

main().catch(console.error);
