import {AttributeName, isAttributeName} from "./AttributeName";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {
    FunctionExpression,
    isFunctionExpression,
    serializeFunctionExpression,
} from "./FunctionExpression";
import {AttributeValue} from "aws-sdk/clients/dynamodb";

export type ComparisonOperand = AttributeName|AttributeValue;

export interface BinaryComparisonPredicate {
    comparedAgainst: ComparisonOperand;
}

export interface EqualityExpressionPredicate extends BinaryComparisonPredicate {
    type: 'Equals';
}

export interface InequalityExpressionPredicate extends BinaryComparisonPredicate {
    type: 'NotEquals';
}

export interface LessThanExpressionPredicate extends BinaryComparisonPredicate {
    type: 'LessThan';
}

export interface LessThanOrEqualToExpressionPredicate extends BinaryComparisonPredicate {
    type: 'LessThanOrEqualTo';
}

export interface GreaterThanExpressionPredicate extends BinaryComparisonPredicate {
    type: 'GreaterThan';
}

export interface GreaterThanOrEqualToExpressionPredicate extends BinaryComparisonPredicate {
    type: 'GreaterThanOrEqualTo';
}

export interface BetweenExpressionPredicate {
    type: 'Between';
    lowerBound: ComparisonOperand;
    upperBound: ComparisonOperand;
}

export interface MembershipExpressionPredicate {
    type: 'Membership';
    values: Array<ComparisonOperand>;
}

export type ConditionExpressionPredicate =
    EqualityExpressionPredicate |
    InequalityExpressionPredicate |
    LessThanExpressionPredicate |
    LessThanOrEqualToExpressionPredicate |
    GreaterThanExpressionPredicate |
    GreaterThanExpressionPredicate |
    GreaterThanOrEqualToExpressionPredicate |
    BetweenExpressionPredicate |
    MembershipExpressionPredicate;

export interface ConditionExpressionSubject {
    subject: ComparisonOperand;
}

export type ConditionExpression =
    ConditionExpressionSubject & ConditionExpressionPredicate |
    AndExpression |
    OrExpression |
    NotExpression |
    FunctionExpression;

export interface AndExpression {
    type: 'And';
    conditions: Array<ConditionExpression>;
}

export interface OrExpression {
    type: 'Or';
    conditions: Array<ConditionExpression>;
}

export interface NotExpression {
    type: 'Not';
    condition: ConditionExpression;
}

export function serializeConditionExpression(
    condition: ConditionExpression,
    attributes: ExpressionAttributes
): string {
    if (isFunctionExpression(condition)) {
        return serializeFunctionExpression(condition, attributes);
    }

    switch (condition.type) {
        case 'Equals':
            return serializeBinaryComparison(condition, attributes, '=');
        case 'NotEquals':
            return serializeBinaryComparison(condition, attributes, '<>');
        case 'LessThan':
            return serializeBinaryComparison(condition, attributes, '<');
        case 'LessThanOrEqualTo':
            return serializeBinaryComparison(condition, attributes, '<=');
        case 'GreaterThan':
            return serializeBinaryComparison(condition, attributes, '>');
        case 'GreaterThanOrEqualTo':
            return serializeBinaryComparison(condition, attributes, '>=');
        case 'Between':
            return `${
                serializeOperand(condition.subject, attributes)
            } BETWEEN ${
                serializeOperand(condition.lowerBound, attributes)
            } AND ${
                serializeOperand(condition.upperBound, attributes)
            }`;
        case 'Membership':
            return `${
                serializeOperand(condition.subject, attributes)
            } IN (${
                condition.values.map(val => serializeOperand(val, attributes))
                    .join(', ')
            })`;
        case 'Not':
            return `NOT (${
                serializeConditionExpression(condition.condition, attributes)
            })`;
        case 'And':
        case 'Or':
            return condition.conditions
                .map(cond => `(${serializeConditionExpression(cond, attributes)})`)
                .join(` ${condition.type.toUpperCase()} `);
    }
}

function serializeBinaryComparison(
    cond: BinaryComparisonPredicate & ConditionExpressionSubject,
    attributes: ExpressionAttributes,
    comparator: string
): string {
    const args: Array<string> = [];
    for (const arg of [cond.subject, cond.comparedAgainst]) {
        args.push(serializeOperand(arg, attributes));
    }

    return `${args[0]} ${comparator} ${args[1]}`;
}

function serializeOperand(
    operand: ComparisonOperand,
    attributes: ExpressionAttributes
): string {
    return isAttributeName(operand)
        ? attributes.addName(operand)
        : attributes.addValue(operand);
}
