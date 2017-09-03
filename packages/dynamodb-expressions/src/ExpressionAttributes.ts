import {
    AttributeName,
    isListIndexAttributeName,
    isMapPropertyAttributeName,
} from './AttributeName';
import {
    AttributeValue,
    ExpressionAttributeNameMap,
    ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';

export class ExpressionAttributes {
    readonly names: ExpressionAttributeNameMap = {};
    readonly values: ExpressionAttributeValueMap = {};

    private readonly nameMap: {[attributeName: string]: string} = {};
    private _ctr = 0;

    addName(attributeName: AttributeName): string {
        if (isListIndexAttributeName(attributeName)) {
            return `${
                this.addName(attributeName.listAttributeName)
            }[${attributeName.index}]`;
        }

        if (isMapPropertyAttributeName(attributeName)) {
            return `${
                this.addName(attributeName.mapAttributeName)
            }.${this.addName(attributeName.propertyAttributeName)}`;
        }

        if (!(attributeName in this.nameMap)) {
            this.nameMap[attributeName] = `#attr${this._ctr++}`;
            this.names[this.nameMap[attributeName]] = attributeName;
        }

        return this.nameMap[attributeName];
    }

    addValue(value: AttributeValue): string {
        const substitution = `:val${this._ctr++}`;
        this.values[substitution] = value;

        return substitution;
    }
}
