import {AttributePath} from "./AttributePath";
import {Marshaller} from "@aws/dynamodb-auto-marshaller";
import {
    AttributeValue,
    ExpressionAttributeNameMap,
    ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';

export class ExpressionAttributes {
    readonly names: ExpressionAttributeNameMap = {};
    readonly values: ExpressionAttributeValueMap = {};
    readonly marshaller = new Marshaller();

    private readonly nameMap: {[attributeName: string]: string} = {};
    private _ctr = 0;

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

    addValue(value: any): string {
        const substitution = `:val${this._ctr++}`;
        this.values[substitution] = this.marshaller.marshallValue(value) as AttributeValue;

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
