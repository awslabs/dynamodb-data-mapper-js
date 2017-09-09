import {marshallValue} from "./marshallItem";
import {OnMissingStrategy} from './OnMissingStrategy';
import {SchemaType} from "./SchemaType";
import {TableDefinition} from './TableDefinition';
import {AttributeValue, UpdateItemInput} from "aws-sdk/clients/dynamodb";
import {
    ConditionExpression,
    ExpressionAttributes,
    MathematicalExpression,
    serializeConditionExpression,
    UpdateExpression,
} from "@aws/dynamodb-expressions";

export interface MarshallUpdateOptions {
    tableDefinition: TableDefinition;
    input: {[key: string]: any};
    onMissing?: OnMissingStrategy;
}

export function marshallUpdate({
    tableDefinition: {tableName: TableName, schema},
    input,
    onMissing = OnMissingStrategy.Remove,
}: MarshallUpdateOptions): UpdateItemInput {
    const attributes = new ExpressionAttributes();
    const expr = new UpdateExpression({attributes});
    const req: UpdateItemInput = {
        TableName,
        Key: {},
    };

    for (const key of Object.keys(schema)) {
        let inputMember = input[key];
        const fieldSchema = schema[key];
        const {attributeName = key} = fieldSchema;

        if (isKey(fieldSchema)) {
            // Marshall keys into the `Keys` property and do not include
            // them in the update expression
            req.Key[attributeName] = marshallValue(fieldSchema, inputMember);
        } else if (isVersionAttribute(fieldSchema)) {
            let condition: ConditionExpression;
            let value: AttributeValue|MathematicalExpression;
            if (inputMember === undefined) {
                condition = {
                    name: 'attribute_not_exists',
                    arguments: [attributeName],
                };
                value = marshallValue(fieldSchema, 0);
            } else {
                condition = {
                    type: 'Equals',
                    subject: attributeName,
                    comparedAgainst: marshallValue(fieldSchema, inputMember),
                };
                value = {
                    leftHandSide: attributeName,
                    operator: '+',
                    rightHandSide: marshallValue(fieldSchema, 1),
                };
            }
            req.ConditionExpression = serializeConditionExpression(condition, attributes);
            expr.set(attributeName, value);
        } else if (inputMember === undefined) {
            if (onMissing === OnMissingStrategy.Remove) {
                expr.remove(attributeName);
            }
        } else {
            expr.set(attributeName, marshallValue(fieldSchema, inputMember));
        }
    }

    req.UpdateExpression = expr.toString();
    req.ExpressionAttributeNames = attributes.names;
    req.ExpressionAttributeValues = attributes.values;

    return req;
}

function isKey(fieldSchema: SchemaType): boolean {
    return (
        fieldSchema.type === 'Binary' ||
        fieldSchema.type === 'Number' ||
        fieldSchema.type === 'String'
    ) && fieldSchema.keyType !== undefined;
}

function isVersionAttribute(fieldSchema: SchemaType): boolean {
    return fieldSchema.type === 'Number'
        && Boolean(fieldSchema.versionAttribute);
}
