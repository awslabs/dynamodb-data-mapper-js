import { QueryOptions } from './namedParameters';
import { Paginator } from './Paginator';
import { getSchema, getTableName } from './protocols';
import { QueryPaginator as BasePaginator } from '@aws/dynamodb-query-iterator';
import {
    marshallConditionExpression,
    marshallKey,
    marshallProjectionExpression,
    ZeroArgumentsConstructor,
} from '@aws/dynamodb-data-marshaller';
import {
    ConditionExpression,
    ConditionExpressionPredicate,
    ExpressionAttributes,
    isConditionExpression,
    isConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';
import { QueryInput, QueryOutput } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

/**
 * Iterates over each page of items returned by a DynamoDB query until no more
 * pages are available.
 */
export class QueryPaginator<T> extends Paginator<T, QueryOutput> {
    constructor(
        client: DynamoDB,
        valueConstructor: ZeroArgumentsConstructor<T>,
        keyCondition: ConditionExpression |
            {[propertyName: string]: ConditionExpressionPredicate|any},
        options: QueryOptions & {tableNamePrefix?: string} = {}
    ) {
        const itemSchema = getSchema(valueConstructor.prototype);

        let {
            filter,
            indexName,
            limit,
            pageSize = limit,
            projection,
            readConsistency,
            scanIndexForward,
            startKey,
            tableNamePrefix: prefix,
        } = options;

        const req: QueryInput = {
            TableName: getTableName(valueConstructor.prototype, prefix),
            ConsistentRead: readConsistency === 'strong',
            ScanIndexForward: scanIndexForward,
            Limit: pageSize,
            IndexName: indexName,
        };

        const attributes = new ExpressionAttributes();
        req.KeyConditionExpression = marshallConditionExpression(
            normalizeKeyCondition(keyCondition),
            itemSchema,
            attributes
        ).expression;

        if (filter) {
            req.FilterExpression = marshallConditionExpression(
                filter,
                itemSchema,
                attributes
            ).expression;
        }

        if (projection) {
            req.ProjectionExpression = marshallProjectionExpression(
                projection,
                itemSchema,
                attributes
            ).expression;
        }

        if (Object.keys(attributes.names).length > 0) {
            req.ExpressionAttributeNames = attributes.names;
        }

        if (Object.keys(attributes.values).length > 0) {
            req.ExpressionAttributeValues = attributes.values;
        }

        if (startKey) {
            req.ExclusiveStartKey = marshallKey(
                itemSchema,
                startKey,
                indexName
            );
        }

        super(
            new BasePaginator(client, req),
            valueConstructor
        );
    }
}

function normalizeKeyCondition(
    keyCondition: ConditionExpression |
        {[key: string]: ConditionExpressionPredicate|any}
): ConditionExpression {
    if (isConditionExpression(keyCondition)) {
        return keyCondition;
    }

    const conditions: Array<ConditionExpression> = [];
    for (const property of Object.keys(keyCondition)) {
        const predicate = keyCondition[property];
        if (isConditionExpressionPredicate(predicate)) {
            conditions.push({
                ...predicate,
                subject: property,
            });
        } else {
            conditions.push({
                type: 'Equals',
                subject: property,
                object: predicate,
            });
        }
    }

    if (conditions.length === 1) {
        return conditions[0];
    }

    return {type: 'And', conditions};
}
