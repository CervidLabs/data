import { Octopus } from "../src/index.js";
import { DataFrame } from "../src/core/DataFrame.js";
import path from 'path';

async function runDemo() {
    try {
        console.log("🐙 Octopus: Starting Full Analytical Pipeline...");

        // 1. Ingest: Load the main Nobel dataset
        const ds = await Octopus.read("./data/nobel.json");
        console.log(`Loaded ${ds.rowCount} records.`);

        // 2. Data Cleaning: Filter and Cast
        // We only want prizes after 1950 and ensure 'year' and 'share' are numbers
        let refined = ds
            .filter(['year'], (y) => parseInt(y) > 1950)
            .cast('year', 'float')
            .cast('share', 'float');

        // 3. Relational Logic: Join with Metadata
        // Creating a secondary DataFrame for enrichment
        const categoryMeta = DataFrame.fromObjects([
            { category: 'physics', weight: 1.5, field_id: 101 },
            { category: 'chemistry', weight: 1.3, field_id: 102 },
            { category: 'economics', weight: 1.2, field_id: 103 },
            { category: 'peace', weight: 1.0, field_id: 104 },
            { category: 'medicine', weight: 1.4, field_id: 105 },
            { category: 'literature', weight: 1.1, field_id: 106 }
        ]);

        console.log("Joining datasets...");
        refined = refined.join(categoryMeta, 'category', 'inner');

        // 4. Vectorized Math: Using the col() executor
        // We calculate a custom 'Impact Score': (share * weight) + (year / 2000)
        console.log("Calculating Vectorized Impact Scores...");
        refined.col('share')
            .mul(refined.col('weight'))
            .add(refined.col('year').div(2000));

        // 5. Analytics: GroupBy and Value Counts
        console.log("\n--- Distribution by Category ---");
        console.table(refined.value_counts('category'));

        console.log("\n--- Summary Statistics ---");
        refined.describe();

        // 6. Inspection: Show top results
        console.log("\n--- Top 5 Processed Records ---");
        refined.select(['year', 'category', 'firstname', 'surname', 'share']).show(5);

        // 7. Export: Async Streaming to CSV and JSON
        console.log("\nExporting results...");
        const outputDir = './data/output';
        
        // Ensure directory exists or use existing one
        await refined.toCSV('nobel_analytics.csv');
        await refined.toJSON('nobel_analytics.json');

        console.log("\n✅ Pipeline completed successfully!");

    } catch (error) {
        console.error("❌ Pipeline failed:", error.message);
    }
}

runDemo();