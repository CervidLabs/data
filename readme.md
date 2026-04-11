# Octopus

### The Bare-Metal Data Engine for Node.js

**Octopus** is a high-performance, vectorized data analysis engine designed to bypass the V8 heap. By utilizing `SharedArrayBuffer` and hardware-level `Atomics`, Octopus enables Node.js to process massive datasets with **sub-second latency** and a **near-constant memory footprint**.

---

## Performance Beyond the Heap

Standard Node.js libraries treat data as objects, leading to frequent **Garbage Collection (GC)** performance degradation. Octopus shifts the paradigm by treating data as raw memory.

* **Zero-Copy Architecture:** Data is mapped directly to `SharedArrayBuffer`. No serialization, no IPC overhead, and zero GC pressure.
* **Vectorized Execution:** High-speed columnar operations optimized for CPU L1/L2 cache hits.
* **True Multithreading:** Parallel Workers write to shared memory using lock-free `Atomics`, achieving massive throughput on multi-core systems.

---

## Performance Benchmark: Octopus vs. Polars

**Dataset:** 7.8M rows NYC Taxi Trip Data (~1.5GB CSV)  
**Environment:** Node.js 22.x | 8 Workers | Local Environment

| Metric | Octopus | Polars (Rust) |
| :--- | :---: | :---: |
| **Total Execution Time** | **2.79s** | 5.98s |
| **Ingestion Speed** | **1.16s** | 1.88s |
| **Processing Throughput** | **~2.8M rows/s** | ~1.6M rows/s |
| **Peak Memory Usage** | **~1.2GB** | ~2.5GB+ |

> **Note:** Octopus outperforms Polars in local Node.js environments by eliminating the cross-language communication overhead (FFI) and leveraging direct V8 memory mapping.

---

## Installation

```bash
npm install octopus-analytics