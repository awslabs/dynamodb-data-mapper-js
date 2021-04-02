import { marshallValue, Schema } from '@aws/dynamodb-data-marshaller';
import {AttributeValue} from "@aws-sdk/client-dynamodb";

/**
 * @internal
 */
export function marshallStartKey(
    schema: Schema,
    startKey: {[key: string]: any}
): {[key: string]: AttributeValue} {
    const key: {[key: string]: AttributeValue} = {};
    for (const propertyName of Object.keys(startKey)) {
        const propSchema = schema[propertyName];
        const { attributeName = propertyName } = propSchema;
        if (propSchema) {
            key[attributeName] = marshallValue(
                propSchema,
                startKey[propertyName]
            )!;
        }
    }

    return key;
}
