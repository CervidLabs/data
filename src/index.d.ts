/**
 * Octopus Engine - High-performance parallel data processing for Node.js
 */

/** Valid comparison operators for filtering */
export type FilterOperator = 'eq' | 'gt' | 'lt' | 'gte' | 'lte' | 'contains' | 'neq';

/** Sort order direction */
export type SortDirection = 'asc' | 'desc';

/** Configuration for the ingestion engine */
export interface OctopusOptions {
    /** Total slots for the internal Hash Table. Recommended: 2x unique keys. Default: 32M */
    indexerCapacity?: number;
    /** Number of records processed locally before atomic synchronization. Default: 2000 */
    batchSize?: number;
    /** Number of worker threads. Defaults to os.cpus().length */
    workers?: number;
    /** If true, enables detailed performance logs in console */
    verbose?: boolean;
}

/** Definition of the filtering logic */
export interface FilterCriteria {
    field: string;
    value: string | number;
    operator: FilterOperator;
}

/** Representación de una fila de datos procesada */
export interface DataRow {
    [key: string]: string | number;
}

/** * Main Data Structure. 
 * Provides a high-level API over the shared TypedArrays.
 */
/** * Internal schema mapping to translate column names to buffer offsets 
 */
export interface SchemaDefinition {
    /** Column name (e.g., 'overall') */
    name: string;
    /** Data type stored in the shared buffer */
    type: 'float64' | 'int32' | 'string';
    /** Index within the specific TypedArray */
    bufferIndex: number;
}

/** * Extended DataFrame with Schema awareness
 */
export interface DataFrame {
    /** * The schema detected during ingestion. 
     * Essential for knowing which fields are available for filtering.
     */
    readonly schema: SchemaDefinition[];

    /** Total number of records in the dataset */
    readonly length: number;

    /** * Returns a specific value without materializing the whole row.
     * Ultra-fast for specific lookups.
     */
    getValue(rowIdx: number, column: string): string | number | null;

    filter(criteria: FilterCriteria): DataFrame;
    sort(column: string, direction?: SortDirection): DataFrame;
    limit(n: number): DataFrame;
    toArray(): DataRow[];
}

/** Core Engine Interface */
export interface OctopusEngine {
    /**
     * Reads and ingests a JSON/CSV dataset using shared memory and atomic sync.
     * @param path File system path to the dataset.
     * @param options Performance and capacity tuning options.
     */
    read(path: string, options?: OctopusOptions): Promise<DataFrame>;
    
    /** Current version of the Octopus engine */
    readonly version: string;
}

/** Main Octopus entry point */
declare const Octopus: OctopusEngine;

export default Octopus;