/**
 * CERVID - High-Performance Data Engine
 * Albetran Tactical Operations © 2026
 */

// --- CORE ---
export { DataFrame, type DataFrameConfig, type ColumnData } from './core/DataFrame.js';

export { Cervid, type CervidReadOptions } from './core/Cervid.js';

export { ParallelExecutor, type ParallelOptions, type TransformDefinition } from './workers/parallel.js';

// --- PREPROCESSING ---
export { Pipeline, type Transformer } from './transformers/Pipeline.js';

export { LabelEncoder } from './transformers/LabelEncoder.js';

export { StringIndexer, type StringIndexerOptions } from './transformers/StringIndexer.js';

export { MinMaxScaler, type MinMaxScalerOptions } from './transformers/MinMaxScaler.js';

export { StandardScaler } from './transformers/StandardScaler.js';

export { OneHotEncoder, type OneHotOptions } from './transformers/OneHotEncoder.js';
