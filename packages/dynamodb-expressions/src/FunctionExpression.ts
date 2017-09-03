import {AttributeName, isAttributeName} from "./AttributeName";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributeValue} from 'aws-sdk/clients/dynamodb';
import {isArrayOf} from "./isArrayOf";

export interface FunctionExpression {
    name: string;
    arguments: Array<AttributeName|AttributeValue>;
}

export function isFunctionExpression(arg: any): arg is FunctionExpression {
    return Boolean(arg)
        && typeof arg === 'object'
        && typeof arg.name === 'string'
        && isArrayOf(
            arg.arguments,
            (val: any): val is AttributeName|AttributeValue =>
                (Boolean(val) && typeof val === 'object') ||
                isAttributeName(val)
        );
}

export function serializeFunctionExpression(
    {arguments: args, name}: FunctionExpression,
    attributes: ExpressionAttributes
): string {
    const expressionSafeArgs = args.map(
        arg => isAttributeName(arg)
            ? attributes.addName(arg)
            : attributes.addValue(arg)
    );
    return `${name}(${expressionSafeArgs.join(', ')})`;
}
