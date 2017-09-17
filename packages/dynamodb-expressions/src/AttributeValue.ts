import {BinaryValue} from "@aws/dynamodb-auto-marshaller";

const MARSHALLED_ATTRIBUTE_VALUE_TAG = 'AmazonDynamoDbAttributeValue';
const EXPECTED_TOSTRING = `[object ${MARSHALLED_ATTRIBUTE_VALUE_TAG}]`;

export class AttributeValue {
    readonly [Symbol.toStringTag] = MARSHALLED_ATTRIBUTE_VALUE_TAG;

    constructor(
        public readonly marshalled: AttributeValueModel
    ) {}

    static isAttributeValue(arg: any): arg is AttributeValue {
        return arg instanceof AttributeValue
            || Object.prototype.toString.call(arg) === EXPECTED_TOSTRING;
    }
}

/**
 * An attribute of type Binary.
 *
 * @example {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])}
 */
export interface BinaryAttributeValue {
    B: BinaryValue;
}

/**
 * An attribute of type Binary Set.
 *
 * @example {
 *  BS: [
 *      Uint8Array.from([0xde, 0xad]),
 *      Uint8Array.from([0xbe, 0xef]),
 *      Uint8Array.from([0xca, 0xfe]),
 *      Uint8Array.from([0xba, 0xbe]),
 *  ],
 * }
 */
export interface BinarySetAttributeValue {
    BS: Array<BinaryValue>;
}

/**
 * An attribute of type Boolean.
 *
 * @example {BOOL: true}
 */
export interface BooleanAttributeValue {
    BOOL: boolean;
}

/**
 * An attribute of type List.
 *
 * @example {L: [{S: "Cookies"}, {S: "Coffee"}, {N: "3.14159"}]}
 */
export interface ListAttributeValue {
    L: Array<AttributeValue>;
}

/**
 * An attribute of type Map.
 *
 * @example {M: {Name: {S: "Joe"}, Age: {N: "35"}}
 */
export interface MapAttributeValue {
    M: {[key: string]: AttributeValue};
}

/**
 * An attribute of type Null.
 */
export interface NullAttributeValue {
    NULL: true;
}

/**
 * An attribute of type Number.
 *
 * Numbers are sent across the network to DynamoDB as strings, to maximize
 * compatibility across languages and libraries. However, DynamoDB treats them
 * as number type attributes for mathematical operations.
 *
 * @example {N: "123.45"}
 */
export interface NumberAttributeValue {
    N: string;
}

/**
 * An attribute of type Number Set.
 *
 * Numbers are sent across the network to DynamoDB as strings, to maximize
 * compatibility across languages and libraries. However, DynamoDB treats them
 * as number type attributes for mathematical operations.
 *
 * @example {NS: ["42.2", "-19", "7.5", "3.14"]}
 */
export interface NumberSetAttributeValue {
    NS: Array<string>;
}

/**
 * An attribute of type String.
 *
 * @example {S: "Hello"}
 */
export interface StringAttributeValue {
    S: string;
}

/**
 * An attribute of type String Set.
 *
 * @example {SS: ["Giraffe", "Hippo" ,"Zebra"]}
 */
export interface StringSetAttributeValue {
    SS: Array<string>;
}

export type AttributeValueModel =
    BinaryAttributeValue |
    BinarySetAttributeValue |
    BooleanAttributeValue |
    ListAttributeValue |
    MapAttributeValue |
    NullAttributeValue |
    NumberAttributeValue |
    NumberSetAttributeValue |
    StringAttributeValue |
    StringSetAttributeValue;
