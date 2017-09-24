import {AttributePath} from "./AttributePath";
import {AttributeValue} from "./AttributeValue";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {FunctionExpression} from "./FunctionExpression";

export type ComparisonOperand = AttributePath|AttributeValue|any;

export interface BinaryComparisonPredicate {
    /**
     * The value against which the comparison subject will be compared.
     */
    object: ComparisonOperand;
}

/**
 * A comparison predicate asserting that the subject and object are equal.
 */
export interface EqualityExpressionPredicate extends BinaryComparisonPredicate {
    type: 'Equals';
}

/**
 * Create an expression predicate asserting that the subject is equal to the
 * predicate.
 */
export function equals(
    operand: ComparisonOperand
): EqualityExpressionPredicate {
    return {
        type: 'Equals',
        object: operand,
    };
}

/**
 * A comparison predicate asserting that the subject and object are not equal.
 */
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

/**
 * A comparison predicate asserting that the subject is less than the object.
 */
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

/**
 * A comparison predicate asserting that the subject is less than or equal to
 * the object.
 */
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

/**
 * A comparison predicate asserting that the subject is greater than the object.
 */
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

/**
 * A comparison predicate asserting that the subject is greater than or equal
 * to the object.
 */
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

/**
 * A comparison predicate asserting that the subject is between two bounds.
 */
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

/**
 * A comparison predicate asserting that the subject is equal to any member of
 * the provided list of values.
 */
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

/**
 * Evaluate whether the provided value is a condition expression predicate.
 */
export function isConditionExpressionPredicate(
    arg: any
): arg is ConditionExpressionPredicate {
    if (arg && typeof arg === 'object') {
        switch (arg.type) {
            case 'Equals':
            case 'NotEquals':
            case 'LessThan':
            case 'LessThanOrEqualTo':
            case 'GreaterThan':
            case 'GreaterThanOrEqualTo':
                return arg.object !== undefined;
            case 'Between':
                return arg.lowerBound !== undefined
                    && arg.upperBound !== undefined;
            case 'Membership':
                return Array.isArray(arg.values);
        }
    }

    return false;
}

export interface ConditionExpressionSubject {
    /**
     * The path to the item attribute containing the subject of the comparison.
     */
    subject: AttributePath|string;
}

export function isConditionExpressionSubject(
    arg: any
): arg is ConditionExpressionSubject {
    return Boolean(arg)
        && typeof arg === 'object'
        && (typeof arg.subject === 'string' || AttributePath.isAttributePath(arg.subject));
}

export type ConditionExpression =
    ConditionExpressionSubject & ConditionExpressionPredicate |
    AndExpression |
    OrExpression |
    NotExpression |
    FunctionExpression;

/**
 * A comparison expression asserting that all conditions in the provided list
 * are true.
 */
export interface AndExpression {
    type: 'And';
    conditions: Array<ConditionExpression>;
}

/**
 * A comparison expression asserting that one or more conditions in the provided
 * list are true.
 */
export interface OrExpression {
    type: 'Or';
    conditions: Array<ConditionExpression>;
}

/**
 * A comparison expression asserting that the provided condition is not true.
 */
export interface NotExpression {
    type: 'Not';
    condition: ConditionExpression;
}

/**
 * Evaluates whether the provided value is a condition expression.
 */
export function isConditionExpression(arg: any): arg is ConditionExpression {
    if (FunctionExpression.isFunctionExpression(arg)) {
        return true;
    }

    if (Boolean(arg) && typeof arg === 'object') {
        switch (arg.type) {
            case 'Not':
                return isConditionExpression(arg.condition);
            case 'And':
            case 'Or':
                if (Array.isArray(arg.conditions)) {
                    for (const condition of arg.conditions) {
                        if (!isConditionExpression(condition)) {
                            return false;
                        }
                    }

                    return true;
                }

                return false;
            default:
                return isConditionExpressionSubject(arg)
                    && isConditionExpressionPredicate(arg);
        }
    }

    return false;
}

/**
 * Convert the provided condition expression object to a string, escaping any
 * values and attributes to expression-safe placeholders whose expansion value
 * will be managed by the provided ExpressionAttributes object.
 */
export function serializeConditionExpression(
    condition: ConditionExpression,
    attributes: ExpressionAttributes
): string {
    if (FunctionExpression.isFunctionExpression(condition)) {
        return condition.serialize(attributes);
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
            if (condition.conditions.length === 1) {
                return serializeConditionExpression(
                    condition.conditions[0],
                    attributes
                );
            }

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
