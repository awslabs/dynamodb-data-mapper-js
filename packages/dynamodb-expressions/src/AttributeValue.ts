import {AttributeValue as BaseAttributeValue} from 'aws-sdk/clients/dynamodb';

const MARSHALLED_ATTRIBUTE_VALUE_TAG = 'AmazonDynamoDbAttributeValue';
const EXPECTED_TOSTRING = `[object ${MARSHALLED_ATTRIBUTE_VALUE_TAG}]`;

export class AttributeValue {
    readonly [Symbol.toStringTag] = MARSHALLED_ATTRIBUTE_VALUE_TAG;

    constructor(
        public readonly marshalled: BaseAttributeValue
    ) {}

    static isAttributeValue(arg: any): arg is AttributeValue {
        return arg instanceof AttributeValue
            || Object.prototype.toString.call(arg) === EXPECTED_TOSTRING;
    }
}
