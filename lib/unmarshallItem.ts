import {Schema} from "./Schema";
import {
    ListType,
    MapType,
    SchemaType, TupleType,
    ZeroArgumentsConstructor,
} from "./SchemaType";
import {BinarySet} from "./BinarySet";
import {
    AttributeMap,
    AttributeValue,
    AttributeValueList,
    Converter,
    NumberSetAttributeValue,
    StringSetAttributeValue,
} from "aws-sdk/clients/dynamodb";
import {InvalidSchemaError} from "./InvalidSchemaError";

export function unmarshallItem<T = {[key: string]: any}>(
    schema: Schema,
    input: AttributeMap,
    valueConstructor?: ZeroArgumentsConstructor<T>
): T {
    const unmarshalled: T = valueConstructor
        ? new valueConstructor()
        : Object.create(null);

    for (let key of Object.keys(schema)) {
        const {attributeName = key} = schema[key];
        if (attributeName in input) {
            (unmarshalled as {[key: string]: any})[key] = unmarshallValue(
                schema[key],
                input[attributeName]
            );
        }
    }

    return unmarshalled;
}

function unmarshallValue(schemaType: SchemaType, input: AttributeValue): any {
    switch (schemaType.type) {
        case 'Binary':
            if (input.NULL) {
                return new Uint8Array(0);
            }

            return input.B;
        case 'BinarySet':
            if (input.NULL) {
                return new BinarySet();
            }

            return typeof input.BS !== 'undefined'
                ? new BinarySet(input.BS as Array<Uint8Array>)
                : undefined;
        case 'Boolean':
            return input.BOOL;
        case 'Collection':
        case 'Hash':
            return Converter.output(input);
        case 'Custom':
            return schemaType.unmarshall(input);
        case 'Date':
            return input.N ? new Date(Number(input.N) * 1000) : undefined;
        case 'Document':
            return input.M
                ? unmarshallItem(
                    schemaType.members,
                    input.M,
                    schemaType.valueConstructor
                ) : undefined;
        case 'List':
            return input.L ? unmarshallList(schemaType, input.L) : undefined;
        case 'Map':
            return input.M ? unmarshallMap(schemaType, input.M) : undefined;
        case 'Null':
            return input.NULL ? null : undefined;
        case 'Number':
            return typeof input.N === 'string' ? Number(input.N) : undefined;
        case 'NumberSet':
            if (input.NULL) {
                return new Set<number>();
            }

            return input.NS ? unmarshallNumberSet(input.NS) : undefined;
        case 'String':
            return input.NULL ? '' : input.S;
        case 'StringSet':
            if (input.NULL) {
                return new Set<string>();
            }

            return input.SS ? unmarshallStringSet(input.SS) : undefined;
        case 'Tuple':
            return input.L ? unmarshallTuple(schemaType, input.L) : undefined;
    }

    throw new InvalidSchemaError(schemaType, 'Unrecognized schema node');
}

function unmarshallList(
    schemaType: ListType,
    input: AttributeValueList
): Array<any> {
    const list: Array<any> = [];
    for (let element of input) {
        list.push(unmarshallValue(schemaType.memberType, element));
    }

    return list;
}

function unmarshallMap(
    schemaType: MapType,
    input: AttributeMap
): Map<string, any> {
    const map = new Map<string, any>();
    for (let key of Object.keys(input)) {
        map.set(key, unmarshallValue(schemaType.memberType, input[key]));
    }

    return map;
}

function unmarshallNumberSet(input: NumberSetAttributeValue): Set<number> {
    const set = new Set<number>();
    for (let number of input) {
        set.add(Number(number));
    }

    return set;
}

function unmarshallStringSet(input: StringSetAttributeValue): Set<string> {
    const set = new Set<string>();
    for (let string of input) {
        set.add(string);
    }

    return set;
}

function unmarshallTuple(
    schemaType: TupleType,
    input: AttributeValueList
): Array<any> {
    const {members} = schemaType;
    const tuple: Array<any> = [];
    for (let i = 0; i < members.length; i++) {
        tuple.push(unmarshallValue(members[i], input[i]));
    }

    return tuple;
}