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
     *
     * @deprecated
     */
    limit?: number;

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
