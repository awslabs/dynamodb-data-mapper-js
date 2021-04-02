import {AttributePath} from "./AttributePath";
import {AttributeValue as AttributeValueClass} from './AttributeValue';
import {
    AttributeValue,
    AttributeValue as AttributeValueModel,
} from '@aws-sdk/client-dynamodb';
import {Marshaller} from "@aws/dynamodb-auto-marshaller";

/**
 * An object that manages expression attribute name and value substitution.
 */
export class ExpressionAttributes {
    readonly names: {[key: string]: string} = {};
    readonly values: {[key: string]: AttributeValue} = {};
    readonly marshaller = new Marshaller();

    private readonly nameMap: {[attributeName: string]: string} = {};
    private _ctr = 0;

    /**
     * Add an attribute path to this substitution context.
     *
     * @returns The substitution value to use in the expression. The same
     * attribute name will always be converted to the same substitution value
     * when supplied to the same ExpressionAttributes object multiple times.
     */
    addName(path: AttributePath|string): string {
        if (AttributePath.isAttributePath(path)) {
            let escapedPath = '';
            for (const element of path.elements) {
                if (element.type === 'AttributeName') {
                    escapedPath += `.${this.addAttributeName(element.name)}`;
                } else {
                    escapedPath += `[${element.index}]`;
                }
            }

            return escapedPath.substring(1);
        }

        return this.addName(new AttributePath(path));
    }

    /**
     * Add an attribute value to this substitution context.
     *
     * @returns The substitution value to use in the expression.
     */
    addValue(value: any): string {
        const modeledAttrValue = AttributeValueClass.isAttributeValue(value)
                ? value.marshalled as AttributeValueModel
                : this.marshaller.marshallValue(value) as AttributeValueModel;

        const substitution = `:val${this._ctr++}`;
        this.values[substitution] = modeledAttrValue;

        return substitution;
    }

    private addAttributeName(attributeName: string): string {
        if (!(attributeName in this.nameMap)) {
            this.nameMap[attributeName] = `#attr${this._ctr++}`;
            this.names[this.nameMap[attributeName]] = attributeName;
        }

        return this.nameMap[attributeName];
    }
}
