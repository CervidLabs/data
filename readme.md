# 🐙 Octopus

**High-Performance Parallel Data Engine for Node.js**

[![Node.js Version](https://img.shields.io/node/v/octopus.svg?style=flat-square)](https://nodejs.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://opensource.org/licenses/MIT)


### The Bare-Metal Data Engine for Node.js

**Octopus** is a high-performance, vectorized data analysis engine designed to bypass the V8 heap. By utilizing `SharedArrayBuffer` and hardware-level `Atomics`, Octopus enables Node.js to process massive datasets with **sub-second latency** and a **near-constant memory footprint**.

---

## Performance Beyond the Heap

Standard Node.js libraries treat data as objects, leading to frequent **Garbage Collection (GC)** performance degradation. Octopus shifts the paradigm by treating data as raw memory.

* **Zero-Copy Architecture:** Data is mapped directly to `SharedArrayBuffer`. No serialization, no IPC overhead, and zero GC pressure.
* **Vectorized Execution:** High-speed columnar operations optimized for CPU L1/L2 cache hits.
* **True Multithreading:** Parallel Workers write to shared memory using lock-free `Atomics`, achieving massive throughput on multi-core systems.

---
## Performance Benchmark: Octopus vs. Polars vs. Pandas

**Dataset:** 7.8M rows NYC Taxi Trip Data (~1.5GB CSV)  
**Environment:** Node.js 22.x | Python 3.11 | 8 Workers (Octopus/Polars) | Local Environment

| Metric | Octopus | Polars (Rust) | Pandas (Python) |
| :--- | :---: | :---: | :---: |
| **Total Execution Time** | **2.79s** | 5.98s | ~35.0s+ |
| **Ingestion Speed** | **1.16s** | 1.88s | ~18.5s |
| **Processing Throughput** | **~2.8M rows/s** | ~1.6M rows/s | ~0.2M rows/s |
| **Peak Memory Usage** | **~1.2GB** | ~2.5GB+ | ~6.0GB+ |

> **Audit Note:** Pandas performance is limited by its single-core nature and high memory overhead (often 4x-10x the raw file size) due to Python object wrapping. Octopus maintains the lead by staying close to the metal with zero-copy buffers.

> **Note:** Octopus outperforms Polars in local Node.js environments by eliminating the cross-language communication overhead (FFI) and leveraging direct V8 memory mapping.

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
    // 1. Parallel Load (8 Workers)
    const ds = await Octopus.read('./large_dataset.csv', { workers: 8 });

    // 2. Vectorized Feature Engineering (Fluent API)
    ds.with_columns([
        {
            name: 'profit_per_mile',
            inputs: ['fare_amount', 'tip_amount', 'trip_distance'],
            formula: (fare, tip, dist) => dist > 0 ? (fare + tip) / dist : 0
        }
    ]);

    // 3. High-Speed Aggregation
    const results = ds.groupByID('PULocationID', 'profit_per_mile');
    
    console.log(results.slice(0, 5));
}

main().catch(console.error);
```
## Key Features

### Columnar Storage Engine
Data is stored in contiguous **TypedArrays** (Float64/Int32). This layout allows the CPU to pre-fetch data significantly faster than row-based object arrays, minimizing cache misses and maximizing throughput.

### Parallel Filter & Map
Operations are distributed across a pool of persistent **Workers**. Octopus automatically partitions the data to ensure all CPU cores are utilized at 100% capacity without memory contention or the overhead of traditional IPC serialization.

### Audit-Ready Accuracy
Designed for financial and scientific audits. Octopus handles UTC offsets, floating-point precision, and outlier filtering with **mathematical parity** against industrial-grade tools like Polars and Pandas.

---

## Roadmap

* **Octopus-Decomposer:** Native, zero-dependency Apache Parquet reader.
* **Lazy Streaming:** Process datasets larger than available RAM via chunked ingestion and iterative processing.
* **SIMD Optimization:** Accelerate mathematical operations by leveraging WebAssembly SIMD instructions for true hardware-level vectorization.

License MIT © 2026 [Villager/Github](https://github.com/villager)