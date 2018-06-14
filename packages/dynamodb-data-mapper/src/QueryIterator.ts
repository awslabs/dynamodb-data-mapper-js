import { Iterator } from './Iterator';
import { QueryOptions } from './namedParameters';
import { QueryPaginator } from './QueryPaginator';
import { ZeroArgumentsConstructor } from '@aws/dynamodb-data-marshaller';
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';
import DynamoDB = require('aws-sdk/clients/dynamodb');

/**
 * Iterates over each item returned by a DynamoDB query until no more pages are
 * available.
 */
export class QueryIterator<T> extends Iterator<T, QueryPaginator<T>> {
    constructor(
        client: DynamoDB,
        valueConstructor: ZeroArgumentsConstructor<T>,
        keyCondition: ConditionExpression |
            {[propertyName: string]: ConditionExpressionPredicate|any},
        options?: QueryOptions & {tableNamePrefix?: string}
    ) {
        super(
            new QueryPaginator(client, valueConstructor, keyCondition, options)
        );
    }
}
