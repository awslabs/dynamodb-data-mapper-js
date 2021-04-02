import {isKey} from './isKey';
import {marshallValue} from './marshallItem';
import {Schema} from './Schema';
import {AttributeValue} from "@aws-sdk/client-dynamodb";

export function marshallKey(
    schema: Schema,
    input: {[key: string]: any},
    indexName?: string
): {[key: string]: AttributeValue} {
    const marshalled: {[key: string]: AttributeValue} = {};

    for (const propertyKey of Object.keys(schema)) {
        const fieldSchema = schema[propertyKey];
        if (isKey(fieldSchema, indexName)) {
            const {attributeName = propertyKey} = fieldSchema;
            const value = marshallValue(fieldSchema, input[propertyKey]);
            if (value) {
                marshalled[attributeName] = value;
            }
        }
    }

    return marshalled;
}
