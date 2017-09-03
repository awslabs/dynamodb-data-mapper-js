import {AttributeName} from './AttributeName';
import {ExpressionAttributes} from './ExpressionAttributes';

export interface ProjectionExpressionConfiguration {
    attributes?: ExpressionAttributes;
}

export class ProjectionExpression {
    readonly attributes: ExpressionAttributes;
    private readonly attributesInExpression: Array<string> = [];

    constructor({
        attributes = new ExpressionAttributes()
    }: ProjectionExpressionConfiguration = {}) {
        this.attributes = attributes;
    }

    addAttribute(name: AttributeName): void {
        this.attributesInExpression.push(this.attributes.addName(name));
    }

    toString() {
        return this.attributesInExpression.join(', ');
    }
}
