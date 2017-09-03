import {AttributeName, isAttributeName} from "./AttributeName";
import {AttributeValue} from "aws-sdk/clients/dynamodb";
import {ExpressionAttributes} from "./ExpressionAttributes";

export class MathematicalExpression {
    leftHandSide: AttributeName|AttributeValue;
    operator: '+'|'-';
    rightHandSide: AttributeName|AttributeValue;
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

function isNameOrValue(arg: any): arg is AttributeName|AttributeValue {
    return (Boolean(arg) && typeof arg === 'object') || isAttributeName(arg);
}

export function serializeMathematicalExpression(
    {leftHandSide, operator, rightHandSide}: MathematicalExpression,
    attributes: ExpressionAttributes
): string {
    const expressionSafeArgs = [leftHandSide, rightHandSide].map(
        arg => isAttributeName(arg)
            ? attributes.addName(arg)
            : attributes.addValue(arg)
    );
    return `${expressionSafeArgs[0]} ${operator} ${expressionSafeArgs[1]}`;
}
