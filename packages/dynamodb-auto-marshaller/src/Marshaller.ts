import {AttributeMap, AttributeValue} from "aws-sdk/clients/dynamodb";
import {BinarySet, BinaryValue} from "./BinarySet";
import {isArrayBuffer} from "./isArrayBuffer";
import {NumberValue} from "./NumberValue";
import {NumberValueSet} from "./NumberValueSet";

export type EmptyHandlingStrategy = 'omit'|'nullify'|'leave';

export type InvalidHandlingStrategy = 'omit'|'throw';

export type UnmarshalledAttributeValue =
    string |
    number |
    NumberValue |
    BinaryValue |
    Set<string> |
    Set<number> |
    Set<NumberValue> |
    BinarySet |
    null |
    boolean |
    UnmarshalledListAttributeValue |
    UnmarshalledMapAttributeValue;

export interface UnmarshalledListAttributeValue extends Array<UnmarshalledAttributeValue> {}

export interface UnmarshalledMapAttributeValue {
    [key: string]: UnmarshalledAttributeValue;
}

export interface MarshallingOptions {
    onEmpty?: EmptyHandlingStrategy;
    onInvalid?: InvalidHandlingStrategy;
    unwrapNumbers?: boolean;
}

export class Marshaller {
    private readonly onEmpty: EmptyHandlingStrategy;
    private readonly onInvalid: InvalidHandlingStrategy;
    private readonly unwrapNumbers: boolean;

    constructor({
        onEmpty = 'leave',
        onInvalid = 'throw',
        unwrapNumbers = false
    }: MarshallingOptions = {}) {
        this.onEmpty = onEmpty;
        this.onInvalid = onInvalid;
        this.unwrapNumbers = unwrapNumbers;
    }

    public marshallItem(item: {[key: string]: any}): AttributeMap {
        const value = this.marshallValue(item);
        if (!(value && value.M) && this.onInvalid === 'throw') {
            throw new Error(
                `Cannot serialize ${typeof item} as an attribute map`
            );
        }

        return value && value.M ? value.M : {};
    }

    public marshallValue(value: any): AttributeValue|undefined {
        switch (typeof value) {
            case 'boolean':
                return {BOOL: value};
            case 'number':
                return {N: value.toString(10)};
            case 'object':
                return this.marshallComplexType(value);
            case 'string':
                return value ? {S: value} : this.handleEmptyString(value);
            case 'undefined':
                return undefined;
            case 'function':
            case 'symbol':
            default:
                if (this.onInvalid === 'throw') {
                    throw new Error(
                        `Cannot serialize values of the ${typeof value} type`
                    );
                }
        }
    }

    public unmarshallItem(item: AttributeMap): UnmarshalledMapAttributeValue {
        return this.unmarshallValue({M: item}) as UnmarshalledMapAttributeValue;
    }

    public unmarshallValue(item: AttributeValue): UnmarshalledAttributeValue {
        if (item.S !== undefined) {
            return item.S;
        }

        if (item.N !== undefined) {
            return this.unwrapNumbers
                ? Number(item.N)
                : new NumberValue(item.N);
        }

        if (item.B !== undefined) {
            return item.B as BinaryValue;
        }

        if (item.BOOL !== undefined) {
            return item.BOOL;
        }

        if (item.NULL !== undefined) {
            return null;
        }

        if (item.SS !== undefined) {
            const set = new Set<string>();
            for (let member of item.SS) {
                set.add(member);
            }
            return set;
        }

        if (item.NS !== undefined) {
            if (this.unwrapNumbers) {
                const set = new Set<number>();
                for (let member of item.NS) {
                    set.add(Number(member));
                }
                return set;
            }

            return new NumberValueSet(
                item.NS.map(numberString => new NumberValue(numberString))
            );
        }

        if (item.BS !== undefined) {
            return new BinarySet(item.BS as Array<BinaryValue>);
        }

        if (item.L !== undefined) {
            return item.L.map(this.unmarshallValue.bind(this));
        }

        const {M = {}} = item;
        return Object.keys(M).reduce(
            (map: UnmarshalledMapAttributeValue, key: string) => {
                map[key] = this.unmarshallValue(M[key]);
                return map;
            },
            {}
        );
    }

    private marshallComplexType(
        value: Set<number|NumberValue|string|BinaryValue> |
            Map<string, any> |
            Iterable<any> |
            {[key: string]: any} |
            null |
            NumberValue
    ): AttributeValue|undefined {
        if (value === null) {
            return {NULL: true};
        }

        if (NumberValue.isNumberValue(value)) {
            return {N: value.toString()};
        }

        if (isBinaryValue(value)) {
            return this.marshallBinaryValue(value);
        }

        if (isSet(value)) {
            return this.marshallSet(value);
        }

        if (isMap(value)) {
            return this.marshallMap(value);
        }

        if (isIterable(value)) {
            return this.marshallList(value);
        }

        return this.marshallObject(value);
    }

    private marshallBinaryValue(binary: BinaryValue): AttributeValue|undefined {
        if (binary.byteLength > 0 || this.onEmpty === 'leave') {
            return {B: binary};
        }

        if (this.onEmpty === 'nullify') {
            return {NULL: true};
        }
    }

    private marshallList(list: Iterable<any>): AttributeValue {
        const values: Array<AttributeValue> = [];
        for (let value of list) {
            const marshalled = this.marshallValue(value);
            if (marshalled) {
                values.push(marshalled);
            }
        }

        return {L: values};
    }

    private marshallMap(map: Map<any, any>): AttributeValue {
        const members: {[key: string]: AttributeValue} = {};
        for (let [key, value] of map) {
            if (typeof key !== 'string') {
                if (this.onInvalid === 'omit') {
                    continue;
                }

                throw new Error(
                    `MapAttributeValues must have strings as keys; ${typeof key} received instead`
                );
            }

            const marshalled = this.marshallValue(value);
            if (marshalled) {
                members[key] = marshalled;
            }
        }

        return {M: members};
    }

    private marshallObject(object: {[key: string]: any}): AttributeValue {
        return {
            M: Object.keys(object).reduce(
                (map: AttributeMap, key: string): AttributeMap => {
                    const marshalled = this.marshallValue(object[key]);
                    if (marshalled) {
                        map[key] = marshalled;
                    }
                    return map;
                },
                {}
            ),
        };
    }

    private marshallSet(arg: Set<any>): AttributeValue|undefined {
        switch (getSetType(arg[Symbol.iterator]().next().value)) {
            case 'binary':
                return this.collectSet(arg, isBinaryEmpty, 'BS', 'binary');
            case 'number':
                return this.collectSet(arg, isNumberEmpty, 'NS', 'number', stringifyNumber);
            case 'string':
                return this.collectSet(arg, isStringEmpty, 'SS', 'string');
            case 'unknown':
                if (this.onInvalid === 'throw') {
                    throw new Error('Sets must be composed of strings,' +
                        ' binary values, or numbers');
                }
                return undefined;
            case 'undefined':
                if (this.onEmpty === 'nullify') {
                    return {NULL: true};
                }
        }
    }

    private collectSet<T, R = T>(
        set: Set<T>,
        isEmpty: (element: T) => boolean,
        tag: 'BS'|'NS'|'SS',
        elementType: 'binary'|'number'|'string',
        transform?: (arg: T) => R
    ): AttributeValue|undefined {
        const values: Array<T|R> = [];
        for (let element of set) {
            if (getSetType(element) !== elementType) {
                if (this.onInvalid === 'omit') {
                    continue;
                }

                throw new Error(
                    `Unable to serialize ${typeof element} as a member of a ${elementType} set`
                );
            }

            if (!isEmpty(element) || this.onEmpty === 'leave') {
                values.push(transform ? transform(element) : element);
            }
        }

        if (values.length > 0 || this.onEmpty === 'leave') {
            return {[tag]: values};
        }

        if (this.onEmpty === 'nullify') {
            return {NULL: true};
        }
    }

    private handleEmptyString(value: string): AttributeValue|undefined {
        switch (this.onEmpty) {
            case 'leave':
                return {S: value};
            case 'nullify':
                return {NULL: true};
        }
    }
}

type SetType = 'string'|'number'|'binary';

function getSetType(arg: any): SetType|'undefined'|'unknown' {
    const type = typeof arg;
    if (type === 'string' || type === 'number' || type === 'undefined') {
        return type;
    }

    if (NumberValue.isNumberValue(arg)) {
        return 'number';
    }

    if (ArrayBuffer.isView(arg) || isArrayBuffer(arg)) {
        return 'binary';
    }

    return 'unknown';
}

function isBinaryEmpty(arg: BinaryValue): boolean {
    return arg.byteLength === 0;
}

function isBinaryValue(arg: any): arg is BinaryValue {
    return ArrayBuffer.isView(arg) || isArrayBuffer(arg);
}

function isIterable(arg: any): arg is Iterable<any> {
    return Boolean(arg) && typeof arg[Symbol.iterator] === 'function';
}

function isMap(arg: any): arg is Map<any, any> {
    return Boolean(arg)
        && Object.prototype.toString.call(arg) === '[object Map]';
}

function isNumberEmpty(): boolean {
    return false;
}

function isSet(arg: any): arg is Set<any> {
    return Boolean(arg)
        && Object.prototype.toString.call(arg) === '[object Set]';
}

function isStringEmpty(arg: string): boolean {
    return arg.length === 0;
}

function stringifyNumber(arg: number|NumberValue): string {
    return arg.toString();
}
