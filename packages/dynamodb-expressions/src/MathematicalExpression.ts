import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

export type MathematicalExpressionOperand = AttributePath|string|number;

const MATHEMATICAL_EXPRESSION_TAG = 'AmazonDynamoDbMathematicalExpression';
const EXPECTED_TOSTRING = `[object ${MATHEMATICAL_EXPRESSION_TAG}]`;

export class MathematicalExpression {
    readonly [Symbol.toStringTag] = MATHEMATICAL_EXPRESSION_TAG;

    constructor(
        readonly lhs: MathematicalExpressionOperand,
        readonly operator: '+'|'-',
        readonly rhs: MathematicalExpressionOperand
    ) {}

    serialize(attributes: ExpressionAttributes) {
        const safeArgs = [this.lhs, this.rhs].map(
            arg => AttributePath.isAttributePath(arg) || typeof arg === 'string'
                ? attributes.addName(arg)
                : attributes.addValue(arg)
        );
        return `${safeArgs[0]} ${this.operator} ${safeArgs[1]}`;
    }

    static isMathematicalExpression(arg: any): arg is MathematicalExpression {
        return arg instanceof MathematicalExpression
            || Object.prototype.toString.call(arg) === EXPECTED_TOSTRING;
    }
}
