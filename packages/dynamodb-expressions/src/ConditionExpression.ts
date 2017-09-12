import {AttributePath} from "./AttributePath";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {
    FunctionExpression,
    isFunctionExpression,
    serializeFunctionExpression,
} from "./FunctionExpression";

export type ComparisonOperand = AttributePath|any;

export interface BinaryComparisonPredicate {
    object: ComparisonOperand;
}

export interface EqualityExpressionPredicate extends BinaryComparisonPredicate {
    type: 'Equals';
}

export function equals(
    operand: ComparisonOperand
): EqualityExpressionPredicate {
    return {
        type: 'Equals',
        object: operand,
    };
}

export interface InequalityExpressionPredicate extends BinaryComparisonPredicate {
    type: 'NotEquals';
}

export function notEquals(
    operand: ComparisonOperand
): InequalityExpressionPredicate {
    return {
        type: 'NotEquals',
        object: operand,
    }
}

export interface LessThanExpressionPredicate extends BinaryComparisonPredicate {
    type: 'LessThan';
}

export function lessThan(
    operand: ComparisonOperand
): LessThanExpressionPredicate {
    return {
        type: 'LessThan',
        object: operand,
    }
}

export interface LessThanOrEqualToExpressionPredicate extends BinaryComparisonPredicate {
    type: 'LessThanOrEqualTo';
}

export function lessThanOrEqualTo(
    operand: ComparisonOperand
): LessThanOrEqualToExpressionPredicate {
    return {
        type: 'LessThanOrEqualTo',
        object: operand,
    }
}

export interface GreaterThanExpressionPredicate extends BinaryComparisonPredicate {
    type: 'GreaterThan';
}

export function greaterThan(
    operand: ComparisonOperand
): GreaterThanExpressionPredicate {
    return {
        type: 'GreaterThan',
        object: operand,
    }
}

export interface GreaterThanOrEqualToExpressionPredicate extends BinaryComparisonPredicate {
    type: 'GreaterThanOrEqualTo';
}

export function greaterThanOrEqualTo(
    operand: ComparisonOperand
): GreaterThanOrEqualToExpressionPredicate {
    return {
        type: 'GreaterThanOrEqualTo',
        object: operand,
    }
}

export interface BetweenExpressionPredicate {
    type: 'Between';
    lowerBound: ComparisonOperand;
    upperBound: ComparisonOperand;
}

export function between(
    lowerBound: ComparisonOperand,
    upperBound: ComparisonOperand
): BetweenExpressionPredicate {
    return {
        type: 'Between',
        lowerBound,
        upperBound,
    }
}

export interface MembershipExpressionPredicate {
    type: 'Membership';
    values: Array<ComparisonOperand>;
}

export function inList(
    ...operands: Array<ComparisonOperand>
): MembershipExpressionPredicate {
    return {
        type: 'Membership',
        values: operands,
    }
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

export function isConditionExpressionPredicate(
    arg: any
): arg is ConditionExpressionPredicate {
    return Boolean(arg)
        && typeof arg === 'object'
        && [
            'Equals',
            'NotEquals',
            'LessThan',
            'LessThanOrEqualTo',
            'GreaterThan',
            'GreaterThanOrEqualTo',
            'Between',
            'Membership',
        ].indexOf(arg.type) > -1;
}

export interface ConditionExpressionSubject {
    subject: AttributePath|string;
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
                attributes.addName(condition.subject)
            } BETWEEN ${
                serializeOperand(condition.lowerBound, attributes)
            } AND ${
                serializeOperand(condition.upperBound, attributes)
            }`;
        case 'Membership':
            return `${
                attributes.addName(condition.subject)
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
    return `${
        attributes.addName(cond.subject)
    } ${comparator} ${
        serializeOperand(cond.object, attributes)
    }`;
}

function serializeOperand(
    operand: ComparisonOperand,
    attributes: ExpressionAttributes
): string {
    return AttributePath.isAttributePath(operand)
        ? attributes.addName(operand)
        : attributes.addValue(operand);
}
