import { marshallValue, Schema } from '@aws/dynamodb-data-marshaller';
import { Key } from 'aws-sdk/clients/dynamodb';

/**
 * @internal
 */
export function marshallStartKey(
    schema: Schema,
    startKey: {[key: string]: any}
): Key {
    const key: Key = {};
    for (const propertyName of Object.keys(startKey)) {
        const propSchema = schema[propertyName];
        if (propSchema) {
            const { attributeName = propertyName } = propSchema;
            key[attributeName] = marshallValue(
                propSchema,
                startKey[propertyName]
            )!;
        }
    }

    return key;
}
