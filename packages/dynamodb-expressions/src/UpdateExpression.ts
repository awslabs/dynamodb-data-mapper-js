import {AttributePath} from "./AttributePath";
import {AttributeValue} from './AttributeValue';
import {ExpressionAttributes} from "./ExpressionAttributes";
import {FunctionExpression} from "./FunctionExpression";
import {MathematicalExpression} from "./MathematicalExpression";

export interface UpdateExpressionConfiguration {
    attributes?: ExpressionAttributes;
}

/**
 * An object representing a DynamoDB update expression.
 */
export class UpdateExpression {
    readonly attributes: ExpressionAttributes;

    private readonly toAdd: {[key: string]: string} = {};
    private readonly toDelete: {[key: string]: string} = {};
    private readonly toRemove = new Set<string>();
    private readonly toSet: {[key: string]: string} = {};

    constructor({
        attributes = new ExpressionAttributes()
    }: UpdateExpressionConfiguration = {}) {
        this.attributes = attributes;
    }

    /**
     * Add a directive to the expression's `add` clause.
     */
    add(path: AttributePath|string, value: any): void {
        this.toAdd[this.attributes.addName(path)]
            = this.attributes.addValue(value);
    }

    /**
     * Add a directive to the expression's `delete` clause.
     */
    delete(path: AttributePath|string, value: any): void {
        this.toDelete[this.attributes.addName(path)]
            = this.attributes.addValue(value);
    }

    /**
     * Add a directive to the expression's `remove` clause.
     */
    remove(path: AttributePath|string): void {
        this.toRemove.add(this.attributes.addName(path));
    }

    /**
     * Add a directive to the expression's `set` clause.
     */
    set(
        path: AttributePath|string,
        value: AttributeValue|FunctionExpression|MathematicalExpression|any
    ): void {
        const lhs = this.attributes.addName(path);
        let rhs: string;
        if (
            FunctionExpression.isFunctionExpression(value) ||
                MathematicalExpression.isMathematicalExpression(value)
        ) {
            rhs = value.serialize(this.attributes);
        } else {
            rhs = this.attributes.addValue(value);
        }

        this.toSet[lhs] = rhs;
    }

    /**
     * Convert the expression to the string format expected by DynamoDB.
     */
    toString(): string {
        const clauses: Array<string> = [];
        for (const [mapping, verb] of [
            [this.toAdd, 'ADD'],
            [this.toDelete, 'DELETE'],
        ] as Array<[{[key: string]: string}, string]>) {
            const keys = Object.keys(mapping);
            if (keys.length > 0) {
                clauses.push(`${verb} ${
                    keys.map(key => `${key} ${mapping[key]}`).join(', ')
                }`);
            }
        }

        const keys = Object.keys(this.toSet);
        if (keys.length > 0) {
            clauses.push(`SET ${
                keys.map(key => `${key} = ${this.toSet[key]}`).join(', ')
            }`);
        }

        if (this.toRemove.size > 0) {
            clauses.push(`REMOVE ${[...this.toRemove].join(', ')}`);
        }

        return clauses.join(' ');
    }
}
