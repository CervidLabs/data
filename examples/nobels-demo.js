import { Octopus } from "../src/index.js";

let ds = await Octopus.read("./data/nobel.json");

// 1. Probar Rename
ds = ds.rename({ year: 'award_year', category: 'nobel_prize' });

// 2. Probar Describe (Estadísticas de la columna 'id' o 'award_year')
console.log("--- Summary Statistics ---");
ds.describe();

// 3. Probar Head y Tail
console.log("--- First 3 rows ---");
ds.head(3).show();

console.log("--- Last 3 rows ---");
ds.tail(3).show();