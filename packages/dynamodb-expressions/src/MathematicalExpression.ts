import {AttributeValue} from "aws-sdk/clients/dynamodb";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

export class MathematicalExpression {
    leftHandSide: AttributePath|string|number;
    operator: '+'|'-';
    rightHandSide: AttributePath|string|number;
}

export function isMathematicalExpression(
    arg: any
): arg is MathematicalExpression {
    return Boolean(arg)
        && typeof arg === 'object'
        && ['-', '+'].indexOf(arg.operator) > 0
        && isNameOrValue(arg.leftHandSide)
        && isNameOrValue(arg.rightHandSide);
}

function isNameOrValue(arg: any): arg is AttributePath|AttributeValue {
    return ['string', 'number'].indexOf(typeof arg) > -1
        || AttributePath.isAttributePath(arg);
}

export function serializeMathematicalExpression(
    {leftHandSide, operator, rightHandSide}: MathematicalExpression,
    attributes: ExpressionAttributes
): string {
    const expressionSafeArgs = [leftHandSide, rightHandSide].map(
        arg => AttributePath.isAttributePath(arg) || typeof arg === 'string'
            ? attributes.addName(arg)
            : attributes.addValue(arg)
    );
    return `${expressionSafeArgs[0]} ${operator} ${expressionSafeArgs[1]}`;
}
