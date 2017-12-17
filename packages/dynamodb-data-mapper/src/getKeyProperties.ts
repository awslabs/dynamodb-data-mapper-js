import { isKey, Schema } from '@aws/dynamodb-data-marshaller';

export function getKeyProperties(schema: Schema): Array<string> {
    const keys: Array<string> = [];
    for (const property of Object.keys(schema).sort()) {
        const fieldSchema = schema[property];
        if (isKey(fieldSchema)) {
            keys.push(fieldSchema.attributeName || property);
        }
    }

    return keys;
}
