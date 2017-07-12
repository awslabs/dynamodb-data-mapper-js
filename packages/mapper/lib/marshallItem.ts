import {Schema} from "./Schema";
import {SchemaType} from "./SchemaType";
import {InvalidValueError} from "./InvalidValueError";
import {InvalidSchemaError} from "./InvalidSchemaError";
import {
    AttributeMap,
    AttributeValue,
    Converter
} from "aws-sdk/clients/dynamodb";
import {BinarySet} from "./BinarySet";

/**
 * Converts a JavaScript object into a DynamoDB Item.
 *
 * @param schema Metadata explaining how the provided input is to be marshalled
 * @param input JavaScript object to convert
 */
export function marshallItem(
    schema: Schema,
    input: {[key: string]: any}
): AttributeMap {
    const marshalled: AttributeMap = {};

    for (let key of Object.keys(input)) {
        if (key in schema) {
            if (input[key] === undefined) {
                continue;
            }

            const {attributeName = key} = schema[key];
            marshalled[attributeName] = marshallValue(schema[key], input[key]);
        }
    }

    return marshalled;
}

/**
 * Converts a value into a DynamoDB AttributeValue.
 *
 * @param schemaType    Metadata outlining how the value is to be understood and
 *                      converted
 * @param input         Value to convert
 */
export function marshallValue(
    schemaType: SchemaType,
    input: any
): AttributeValue {
    if (schemaType.type === 'Binary') {
        if (input.byteLength === 0) {
            return {NULL: true};
        }

        return {B: marshallBinary(input)};
    }

    if (schemaType.type === 'BinarySet') {
        if (!(input instanceof BinarySet)) {
            input = new BinarySet(input);
        }

        return marshallSet(
            input,
            marshallBinary,
            (bin: Uint8Array) => bin.byteLength === 0,
            'BS'
        );
    }

    if (schemaType.type === 'Boolean') {
        return {BOOL: Boolean(input)};
    }

    if (schemaType.type === 'Custom') {
        return schemaType.marshall(input);
    }

    if (schemaType.type === 'Collection') {
        const collected: Array<AttributeValue> = [];
        for (let element of input) {
            collected.push(
                Converter.input(element, {convertEmptyValues: true})
            );
        }

        return {L: collected};
    }

    if (schemaType.type === 'Date') {
        let date: Date;
        if (typeof input === 'string') {
            date = new Date(input);
        } else if (typeof input === 'number') {
            date = new Date(input * 1000);
        } else if (isDate(input)) {
            date = input;
        } else {
            throw new InvalidValueError(
                input,
                'Unable to convert value to date'
            );
        }

        return {N: marshallNumber(Math.floor(date.valueOf() / 1000))};
    }

    if (schemaType.type === 'Document') {
        return {M: marshallItem(schemaType.members, input)};
    }

    if (schemaType.type === 'Hash') {
        return {M: Converter.marshall(input, {convertEmptyValues: true})};
    }

    if (schemaType.type === 'List') {
        const elements = [];
        for (let member of input) {
            elements.push(marshallValue(schemaType.memberType, member));
        }
        return {L: elements};
    }

    if (schemaType.type === 'Map') {
        const marshalled: AttributeMap = {};
        if (typeof input[Symbol.iterator] === 'function') {
            for (let [key, value] of input) {
                marshalled[key] = marshallValue(schemaType.memberType, value);
            }
        } else if (typeof input === 'object') {
            for (let key of Object.keys(input)) {
                marshalled[key] = marshallValue(
                    schemaType.memberType,
                    input[key]
                );
            }
        } else {
            throw new InvalidValueError(
                input,
                'Unable to convert value to map'
            );
        }

        return {M: marshalled};
    }

    if (schemaType.type === 'Null') {
        return {NULL: true};
    }

    if (schemaType.type === 'Number') {
        return {N: marshallNumber(input)};
    }

    if (schemaType.type === 'NumberSet') {
        if (!(input instanceof Set)) {
            input = new Set<number>(input);
        }

        return marshallSet(
            input,
            marshallNumber,
            () => false,
            'NS'
        );
    }

    if (schemaType.type === 'String') {
        const string = marshallString(input);
        if (string.length === 0) {
            return {NULL: true};
        }

        return {S: string};
    }

    if (schemaType.type === 'StringSet') {
        if (!(input instanceof Set)) {
            input = new Set<string>(input);
        }

        return marshallSet(
            input,
            marshallString,
            (string: string) => string.length === 0,
            'SS'
        );
    }

    if (schemaType.type === 'Tuple') {
        return {
            L: schemaType.members.map((
                type: SchemaType,
                index: number
            ) => marshallValue(type, input[index])),
        }
    }

    throw new InvalidSchemaError(schemaType, 'Unrecognized schema node');
}

function marshallBinary(input: ArrayBuffer|ArrayBufferView): Uint8Array {
    if (ArrayBuffer.isView(input)) {
        return new Uint8Array(
            input.buffer,
            input.byteOffset,
            input.byteLength
        );
    }

    if (isArrayBuffer(input)) {
        return new Uint8Array(input);
    }

    throw new InvalidValueError(
        input,
        'Unable to serialize provided value as binary'
    );
}

function marshallNumber(input: number): string {
    return input.toString(10);
}

function marshallString(input: {toString(): string}): string {
    return input.toString();
}

function marshallSet<InputType, MarshalledElementType>(
    value: Iterable<InputType>,
    marshaller: (element: InputType) => MarshalledElementType,
    isEmpty: (member: MarshalledElementType) => boolean,
    setTag: 'BS'|'NS'|'SS'
): AttributeValue {
    const collected: Array<MarshalledElementType> = [];
    for (let member of value) {
        const marshalled = marshaller(member);
        if (isEmpty(marshalled)) {
            // DynamoDB sets cannot contain empty values
            continue;
        }

        collected.push(marshalled);
    }

    if (collected.length === 0) {
        return {NULL: true};
    }

    return {[setTag]: collected};
}

function isArrayBuffer(arg: any): arg is ArrayBuffer {
    return typeof ArrayBuffer === 'function'
        && (
            arg instanceof ArrayBuffer ||
            Object.prototype.toString.call(arg) === '[object ArrayBuffer]'
        );
}

function isDate(arg: any): arg is Date {
    return arg instanceof Date
        || Object.prototype.toString.call(arg) === '[object Date]';
}