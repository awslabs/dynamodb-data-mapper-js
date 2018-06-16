import { ReadConsistencyConfiguration } from './ReadConsistencyConfiguration';
import { StringToAnyObjectMap } from '../constants';
import { ZeroArgumentsConstructor } from '@aws/dynamodb-data-marshaller';
import {
    ConditionExpression,
    ProjectionExpression,
} from '@aws/dynamodb-expressions';

export interface BaseScanOptions extends ReadConsistencyConfiguration {
    /**
     * A string that contains conditions that DynamoDB applies after the Query
     * operation, but before the data is returned to you. Items that do not
     * satisfy the FilterExpression criteria are not returned.
     *
     * A FilterExpression does not allow key attributes. You cannot define a
     * filter expression based on a partition key or a sort key.
     */
    filter?: ConditionExpression;

    /**
     * The name of an index to query. This index can be any local secondary
     * index or global secondary index on the table.
     */
    indexName?: string;

    /**
     * The maximum number of items to fetch per page of results.
     */
    pageSize?: number;

    /**
     * The item attributes to get.
     */
    projection?: ProjectionExpression;
}

export interface CtorBearer<T extends StringToAnyObjectMap = StringToAnyObjectMap> {
    /**
     * A constructor that creates objects representing one record returned by
     * the query operation.
     */
    valueConstructor: ZeroArgumentsConstructor<T>;
}

export interface BaseSequentialScanOptions extends BaseScanOptions {
    /**
     * The maximum number of items to fetch over all pages of scan.
     */
    limit?: number;

    /**
     * For a parallel Scan request, Segment identifies an individual segment to
     * be scanned by an application worker.
     *
     * Segment IDs are zero-based, so the first segment is always 0. For
     * example, if you want to use four application threads to scan a table or
     * an index, then the first thread specifies a Segment value of 0, the
     * second thread specifies 1, and so on.
     */
    segment?: number;

    /**
     * The primary key of the first item that this operation will evaluate.
     */
    startKey?: {[key: string]: any};

    /**
     * The number of application workers that will perform the scan.
     *
     * Must be an integer between 1 and 1,000,000
     */
    totalSegments?: number;
}

export interface ScanOptions extends BaseSequentialScanOptions {
    segment?: undefined;
    totalSegments?: undefined;
}

/**
 * Pagination state for a scan segment for which the first page has not yet been
 * retrieved.
 */
export interface UninitializedScanState {
    initialized: false;
    lastEvaluatedKey?: undefined;
}

/**
 * Pagination state for a scan segment for which one or more pages have been
 * retrieved. If `lastEvaluatedKey` is defined, there are more pages to fetch;
 * otherwise, all pages for this segment have been returned.
 */
export interface InitializedScanState {
    initialized: true;
    lastEvaluatedKey?: {[attributeName: string]: any};
}

export type ScanState = UninitializedScanState|InitializedScanState;

/**
 * ParallelScanState is represented as an array whose length is equal to the
 * number of segments being scanned independently, with each segment's state
 * being stored at the array index corresponding to its segment number.
 *
 * Segment state is represented with a tagged union with the following keys:
 *   - `initialized` -- whether the first page of results has been retrieved
 *   - `lastEvaluatedKey` -- the key to provide (if any) when requesting the
 *      next page of results.
 *
 * If `lastEvaluatedKey` is undefined and `initialized` is true, then all pages
 * for the given segment have been returned.
 */
export type ParallelScanState = Array<ScanState>;

export interface ParallelScanOptions extends BaseScanOptions {
    /**
     * The segment identifier must not be supplied when initiating a parallel
     * scan. This identifier will be created for each worker on your behalf.
     */
    segment?: undefined;

    /**
     * The point from which a parallel scan should resume.
     */
    scanState?: ParallelScanState;
}

/**
 * @deprecated
 */
export type ScanParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> = ScanOptions & CtorBearer<T>;

export interface ParallelScanWorkerOptions extends BaseSequentialScanOptions {
    segment: number;
    totalSegments: number;
}

/**
 * @deprecated
 */
export type ParallelScanWorkerParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> = ParallelScanWorkerOptions & CtorBearer<T>;

/**
 * @deprecated
 */
export type ParallelScanParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> = BaseScanOptions & CtorBearer<T> & {
    /**
     * The number of application workers that will perform the scan.
     *
     * Must be an integer between 1 and 1,000,000
     */
    segments: number;
};

/**
 * @internal
 */
export type SequentialScanOptions = (ScanOptions|ParallelScanWorkerOptions) & {tableNamePrefix?: string};


