import { SequentialScanOptions } from './namedParameters';
import { getSchema, getTableName } from './protocols';
import {
    marshallConditionExpression,
    marshallKey,
    marshallProjectionExpression,
    ZeroArgumentsConstructor,
} from '@aws/dynamodb-data-marshaller';
import { ExpressionAttributes } from '@aws/dynamodb-expressions';
import { ScanInput } from 'aws-sdk/clients/dynamodb';

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
        limit,
        pageSize = limit,
        projection,
        readConsistency,
        segment,
        startKey,
        tableNamePrefix: prefix,
        totalSegments,
    } = options;

    const req: ScanInput = {
        TableName: getTableName(valueConstructor.prototype, prefix),
        ConsistentRead: readConsistency === 'strong',
        Limit: pageSize,
        IndexName: indexName,
        Segment: segment,
        TotalSegments: totalSegments,
    };

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
        req.ExclusiveStartKey = marshallKey(schema, startKey, indexName);
    }

    return req;
}
