import { marshallStartKey } from './marshallStartKey';
import { SequentialScanOptions } from './namedParameters';
import { getSchema, getTableName } from './protocols';
import {
    marshallConditionExpression,
    marshallProjectionExpression,
    ZeroArgumentsConstructor,
} from '@awslabs-community-fork/dynamodb-data-marshaller';
import { ExpressionAttributes } from '@awslabs-community-fork/dynamodb-expressions';
import { ScanInput } from '@aws-sdk/client-dynamodb';

/**
 * @internal
 */
export function buildScanInput<T>(
    valueConstructor: ZeroArgumentsConstructor<T>,
    options: SequentialScanOptions = {}
): ScanInput {
    const {
        filter,
        indexName,
        pageSize,
        projection,
        readConsistency,
        segment,
        startKey,
        tableNamePrefix: prefix,
        totalSegments,
    } = options;

    const req: ScanInput = {
        TableName: getTableName(valueConstructor.prototype, prefix),
        Limit: pageSize,
        IndexName: indexName,
        Segment: segment,
        TotalSegments: totalSegments,
    };

    if (readConsistency === 'strong') {
        req.ConsistentRead = true;
    }

    const schema = getSchema(valueConstructor.prototype);

    const attributes = new ExpressionAttributes();

    if (filter) {
        req.FilterExpression = marshallConditionExpression(
            filter,
            schema,
            attributes
        ).expression;
    }

    if (projection) {
        req.ProjectionExpression = marshallProjectionExpression(
            projection,
            schema,
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
        req.ExclusiveStartKey = marshallStartKey(schema, startKey);
    }

    return req;
}
