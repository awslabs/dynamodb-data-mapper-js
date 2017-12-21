import DynamoDB = require("aws-sdk/clients/dynamodb");
import {
    OnMissingStrategy,
    ReadConsistency,
    StringToAnyObjectMap,
} from "./constants";
import {
    Schema,
    ZeroArgumentsConstructor,
} from "@aws/dynamodb-data-marshaller";
import {
    ConditionExpression,
    ConditionExpressionPredicate,
    ProjectionExpression,
} from "@aws/dynamodb-expressions";

export interface DataMapperConfiguration {
    /**
     * The low-level DynamoDB client to use to execute API operations.
     */
    client: DynamoDB;

    /**
     * The default read consistency to use when loading items. If not specified,
     * 'eventual' will be used.
     */
    readConsistency?: ReadConsistency;

    /**
     * Whether operations should NOT by default honor the version attribute
     * specified in the schema by incrementing the attribute and preventing the
     * operation from taking effect if the local version is out of date.
     */
    skipVersionCheck?: boolean;

    /**
     * A prefix to apply to all table names.
     */
    tableNamePrefix?: string;
}

export interface BatchGetOptions extends ReadConsistencyConfiguration {
    /**
     * Options to apply to specific tables when performing a batch get operation
     * that reads from multiple tables.
     */
    perTableOptions?: {
        [key: string]: GetOptions;
    };
}

export interface BatchGetTableOptions extends GetOptions {
    /**
     * The schema to use when mapping the supplied `projection` option to the
     * attribute names used in DynamoDB.
     *
     * This parameter is only necessary if a batch contains items from multiple
     * classes that map to the *same* table using *different* property names to
     * represent the same DynamoDB attributes.
     *
     * If not supplied, the schema associated with the first item associated
     * with a given table will be used in its place.
     */
    projectionSchema?: Schema;
}

export interface DeleteOptions {
    /**
     * A condition on which this delete operation's completion will be
     * predicated.
     */
    condition?: ConditionExpression;

    /**
     * The values to return from this operation.
     */
    returnValues?: 'ALL_OLD'|'NONE';

    /**
     * Whether this operation should NOT honor the version attribute specified
     * in the schema by incrementing the attribute and preventing the operation
     * from taking effect if the local version is out of date.
     */
    skipVersionCheck?: boolean;
}

export interface DeleteParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends DeleteOptions {
    /**
     * The item being deleted.
     */
    item: T;
}

export interface GetOptions extends ReadConsistencyConfiguration {
    /**
     * The item attributes to get.
     */
    projection?: ProjectionExpression;
}

export interface GetParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends GetOptions {
    /**
     * The item being loaded.
     */
    item: T;
}

export interface PutOptions {
    /**
     * A condition on whose evaluation this put operation's completion will be
     * predicated.
     */
    condition?: ConditionExpression;

    /**
     * Whether this operation should NOT honor the version attribute specified
     * in the schema by incrementing the attribute and preventing the operation
     * from taking effect if the local version is out of date.
     */
    skipVersionCheck?: boolean;
}

export interface PutParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends PutOptions {
    /**
     * The object to be saved.
     */
    item: T;
}

export interface QueryOptions extends ReadConsistencyConfiguration {
    /**
     * A condition expression that DynamoDB applies after the Query operation,
     * but before the data is returned to you. Items that do not satisfy the
     * FilterExpression criteria are not returned.
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

    /**
     * Specifies the order for index traversal: If true, the traversal is
     * performed in ascending order; if false, the traversal is performed in
     * descending order.
     *
     * Items with the same partition key value are stored in sorted order by
     * sort key. If the sort key data type is Number, the results are stored in
     * numeric order. For type String, the results are stored in order of ASCII
     * character code values. For type Binary, DynamoDB treats each byte of the
     * binary data as unsigned.
     */
    scanIndexForward?: boolean;

    /**
     * The primary key of the first item that this operation will evaluate.
     */
    startKey?: {[key: string]: any};
}

export interface QueryParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends QueryOptions {
    /**
     * The condition that specifies the key value(s) for items to be retrieved
     * by the Query action.
     */
    keyCondition: ConditionExpression |
        {[propertyName: string]: ConditionExpressionPredicate|any};

    /**
     * A constructor that creates objects representing one record returned by
     * the query operation.
     */
    valueConstructor: ZeroArgumentsConstructor<T>;
}

export interface ReadConsistencyConfiguration {
    /**
     * The read consistency to require when reading from DynamoDB.
     */
    readConsistency?: ReadConsistency;
}

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

export type ScanParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> = ScanOptions & CtorBearer<T>;

export interface ParallelScanWorkerOptions extends BaseSequentialScanOptions {
    segment: number;
    totalSegments: number;
}

export type ParallelScanWorkerParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> = ParallelScanWorkerOptions & CtorBearer<T>;

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

export interface UpdateOptions {
    /**
     * A condition on whose evaluation this update operation's completion will
     * be predicated.
     */
    condition?: ConditionExpression;

    /**
     * Whether the absence of a value defined in the schema should be treated as
     * a directive to remove the property from the item.
     */
    onMissing?: OnMissingStrategy;

    /**
     * Whether this operation should NOT honor the version attribute specified
     * in the schema by incrementing the attribute and preventing the operation
     * from taking effect if the local version is out of date.
     */
    skipVersionCheck?: boolean;
}

export interface UpdateParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends UpdateOptions {
    /**
     * The object to be saved.
     */
    item: T;
}
