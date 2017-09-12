import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

export interface FunctionExpression {
    name: string;
    arguments: Array<AttributePath|any>;
}

export function isFunctionExpression(arg: any): arg is FunctionExpression {
    return Boolean(arg)
        && typeof arg === 'object'
        && typeof arg.name === 'string'
        && Array.isArray(arg.arguments);
}

export function serializeFunctionExpression(
    {arguments: args, name}: FunctionExpression,
    attributes: ExpressionAttributes
): string {
    const expressionSafeArgs = args.map(
        arg => AttributePath.isAttributePath(arg)
            ? attributes.addName(arg)
            : attributes.addValue(arg)
    );
    return `${name}(${expressionSafeArgs.join(', ')})`;
}
