import { ReadConsistencyConfiguration } from './ReadConsistencyConfiguration';
import { StringToAnyObjectMap } from '../constants';
import { ZeroArgumentsConstructor } from '@aws/dynamodb-data-marshaller';
import {
    ConditionExpression,
    ConditionExpressionPredicate,
    ProjectionExpression,
} from '@aws/dynamodb-expressions';

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
     * The maximum number of items to fetch over all pages of the query.
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
     * The primary key of the first item that this operation will evaluate. When
     * querying an index, only the `lastEvaluatedKey` derived from a previous
     * query operation on the same index should be supplied for this parameter.
     */
    startKey?: {[key: string]: any};
}

/**
 * @deprecated
 */
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
