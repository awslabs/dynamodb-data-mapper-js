import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

const FUNCTION_EXPRESSION_TAG = 'AmazonDynamoDbFunctionExpression';
const EXPECTED_TOSTRING = `[object ${FUNCTION_EXPRESSION_TAG}]`;

export class FunctionExpression {
    readonly [Symbol.toStringTag] = FUNCTION_EXPRESSION_TAG;
    readonly args: Array<AttributePath|any>;

    constructor(
        readonly name: string,
        ...args: Array<AttributePath|any>
    ) {
        this.args = args;
    }

    serialize(attributes: ExpressionAttributes) {
        const expressionSafeArgs = this.args.map(
            arg => AttributePath.isAttributePath(arg)
                ? attributes.addName(arg)
                : attributes.addValue(arg)
        );
        return `${this.name}(${expressionSafeArgs.join(', ')})`;
    }

    static isFunctionExpression(arg: any): arg is FunctionExpression {
        return arg instanceof FunctionExpression
            || Object.prototype.toString.call(arg) === EXPECTED_TOSTRING;
    }
}
