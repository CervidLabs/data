# 🐙 Octopus

**High-Performance Data Engine for Node.js**

> Process millions of rows in seconds — directly in Node.js.

[![Node.js Version](https://img.shields.io/node/v/octopus.svg?style=flat-square)](https://nodejs.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://opensource.org/licenses/MIT)

---

## The Bare-Metal Data Engine

**Octopus** is a high-performance, vectorized data engine designed to push Node.js beyond traditional limits. By leveraging `SharedArrayBuffer` and low-level `Atomics`, Octopus processes massive datasets with **extremely low latency** and a **predictable memory footprint**.

Unlike traditional libraries that rely on JavaScript objects, Octopus operates directly on raw memory.

---

## Why Octopus?

- ⚡ **Built for Node.js** — no Python bindings, no native dependencies  
- 🧠 **Zero-Copy Architecture** — no serialization, no GC pressure  
- 🧵 **True Multithreading** — parallel workers using shared memory  
- 📊 **Vectorized Execution** — optimized for CPU cache efficiency  
- 📉 **Predictable Memory Usage** — no hidden overhead  

---

## Performance Beyond the Heap

Traditional Node.js data processing suffers from:
- Heavy object allocation  
- Frequent garbage collection (GC)  
- Poor CPU cache utilization  

Octopus solves this by using **TypedArrays** for columnar storage, **SharedArrayBuffer** for zero-copy memory access, and **Atomics** for lock-free parallelism.

---

## Benchmark: Octopus vs Polars vs Pandas

**Dataset:** 7.8M rows NYC Taxi Trip Data (~1.5GB CSV)  
**Environment:** Node.js 22.x | Python 3.11 | 8 Workers | Local Machine  

| Metric | Octopus | Polars (Rust) | Pandas (Python) |
| :--- | :---: | :---: | :---: |
| **Total Execution Time** | **2.79s** | 5.98s | ~35.0s |
| **Ingestion Speed** | **1.16s** | 1.88s | ~18.5s |
| **Processing Throughput** | **~2.8M rows/s** | ~1.6M rows/s | ~0.2M rows/s |
| **Peak Memory Usage** | **~1.2GB** | ~2.5GB+ | ~6.0GB+ |

> Octopus achieves high performance by staying close to the metal and avoiding cross-language overhead (FFI).

---
## Installation

```bash
npm install octopus
```

---

## Quick Start

```javascript
import { Octopus } from 'octopus';

async function main() {
    const ds = await Octopus.read('./large_dataset.csv', { workers: 8 });

    ds.with_columns([
        {
            name: 'profit_per_mile',
            inputs: ['fare_amount', 'tip_amount', 'trip_distance'],
            formula: (fare, tip, dist) => dist > 0 ? (fare + tip) / dist : 0
        }
    ]);

    const results = ds.groupByID('PULocationID', 'profit_per_mile');

    console.log(results.slice(0, 5));
}

main().catch(console.error);
```
---

## Example Output
```json
[
  { group: 84, avg: 2165.64 },
  { group: 132, avg: 1987.21 }
]
```
---

## Architecture Overview

### Columnar Storage Engine
Data is stored in contiguous memory using **TypedArrays**, allowing fast sequential access and optimal CPU cache usage. This structure minimizes memory fragmentation and maximizes data locality.

### Parallel Execution Engine
Workloads are split across persistent **Workers**, achieving near 100% CPU utilization. By avoiding the traditional "main thread bottleneck," Octopus can process millions of rows per second without blocking the event loop.

### Zero-Copy Memory Model
All workers operate on shared memory via **SharedArrayBuffer**, eliminating data duplication and reducing memory pressure. This allows for seamless thread communication without the overhead of IPC serialization.

---
## Key Features

* **Native Parquet Engine:** High-performance, zero-copy binary decoding. Octopus reads Parquet files directly into columnar memory without intermediate object conversion.
* **Streaming Engine:** Out-of-core processing architecture. Analyze datasets that exceed physical RAM limits by leveraging chunked ingestion and shared memory buffers.
* **Columnar Storage:** Data is stored in contiguous TypedArrays, optimizing CPU cache hits and minimizing memory fragmentation.
## Roadmap


* **Binary-Native Ingestion:** Implementing direct mapping for binary formats (Parquet/Arrow) to eliminate string-parsing bottlenecks and achieve true zero-copy ingestion.
* **Query Planner & Optimization Layer:** Automated predicate pushdown and execution graph optimization to skip unnecessary data processing.
* **SIMD / WASM Acceleration:** Leveraging hardware-level vectorization via WebAssembly for ultra-fast mathematical operations on columnar data.
* **Advanced Streaming:** Enhancing out-of-core processing for multi-terabyte datasets that far exceed physical RAM.
---

## License
MIT © 2026 [Villager/Github](https://github.com/villager)