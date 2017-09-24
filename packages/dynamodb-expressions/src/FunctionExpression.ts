import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

const FUNCTION_EXPRESSION_TAG = 'AmazonDynamoDbFunctionExpression';
const EXPECTED_TOSTRING = `[object ${FUNCTION_EXPRESSION_TAG}]`;

/**
 * An object representing a DynamoDB function expression.
 */
export class FunctionExpression {
    readonly [Symbol.toStringTag] = FUNCTION_EXPRESSION_TAG;
    readonly args: Array<AttributePath|any>;

    constructor(
        readonly name: string,
        ...args: Array<AttributePath|any>
    ) {
        this.args = args;
    }

    /**
     * Convert the function expression represented by this object into the
     * string format expected by DynamoDB. Any attribute names and values
     * will be replaced with substitutions supplied by the provided
     * ExpressionAttributes object.
     */
    serialize(attributes: ExpressionAttributes) {
        const expressionSafeArgs = this.args.map(
            arg => AttributePath.isAttributePath(arg)
                ? attributes.addName(arg)
                : attributes.addValue(arg)
        );
        return `${this.name}(${expressionSafeArgs.join(', ')})`;
    }

    /**
     * Evaluate whether the provided value is a FunctionExpression object.
     */
    static isFunctionExpression(arg: any): arg is FunctionExpression {
        return arg instanceof FunctionExpression
            || Object.prototype.toString.call(arg) === EXPECTED_TOSTRING;
    }
}
