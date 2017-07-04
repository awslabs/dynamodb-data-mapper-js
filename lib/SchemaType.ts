import {AttributeValue} from 'aws-sdk/clients/dynamodb';
import {Schema} from './Schema';

export const TypeTags = {
    Binary: 'Binary',
    BinarySet: 'BinarySet',
    Boolean: 'Boolean',
    Collection: 'Collection',
    Custom: 'Custom',
    Date: 'Date',
    Document: 'Document',
    Hash: 'Hash',
    List: 'List',
    Map: 'Map',
    Null: 'Null',
    Number: 'Number',
    NumberSet: 'NumberSet',
    String: 'String',
    StringSet: 'StringSet',
    Tuple: 'Tuple',
};

export type TypeTag = keyof typeof TypeTags;

export interface BaseType {
    type: TypeTag;
    attributeName?: string;
}

function isBaseType(arg: any): arg is BaseType {
    return Boolean(arg) && typeof arg === 'object'
        && typeof arg.type === 'string'
        && arg.type in TypeTags
        && ['string', 'undefined'].indexOf(typeof arg.attributeName) > -1;
}

export interface MemberType {
    attributeName?: undefined;
}

export enum KeyType {
    HASH = 'HASH',
    RANGE = 'RANGE',
}

export interface KeyConfiguration {
    type: KeyType;
}

function isKeyConfiguration(arg: any): boolean {
    return Boolean(arg) && arg.type in KeyType;
}

export interface IndexKeyConfiguration extends KeyConfiguration {
    indexName: string;
}

function isIndexKeyConfiguration(arg: any): boolean {
    return isKeyConfiguration(arg) && typeof arg.indexName === 'string';
}

export interface KeyableType {
    keyConfiguration?: KeyConfiguration;
    indexKeyConfigurations?: Array<IndexKeyConfiguration>,
}

function isKeyableType(arg: object): boolean {
    const {keyConfiguration, indexKeyConfigurations} = arg as any;

    return (
        keyConfiguration === undefined ||
        isKeyConfiguration(keyConfiguration)
    ) && (
        indexKeyConfigurations === undefined ||
        (
            Array.isArray(indexKeyConfigurations) &&
            indexKeyConfigurations.map(isIndexKeyConfiguration)
                .filter(b => !b)
                .length === 0
        )
    );
}

export interface BinaryType extends BaseType, KeyableType {
    type: 'Binary';
}

export interface BinarySetType extends BaseType {
    type: 'BinarySet';
}

export interface BooleanType extends BaseType {
    type: 'Boolean';
}

export interface CollectionType extends BaseType {
    type: 'Collection';
}

export interface CustomType<JsType> extends BaseType {
    type: 'Custom';
    marshall: (input: JsType)=> AttributeValue;
    unmarshall: (persistedValue: AttributeValue) => JsType;
}

export interface DateType extends BaseType {
    type: 'Date';
}

export interface ZeroArgumentsConstructor<T = any> {
    new (): T;
}

export interface DocumentType extends BaseType {
    type: 'Document';
    members: Schema;
    valueConstructor?: ZeroArgumentsConstructor;
}

export interface HashType extends BaseType {
    type: 'Hash';
}

export interface ListType extends BaseType {
    type: 'List';
    memberType: SchemaType & MemberType;
}

export interface MapType extends BaseType {
    type: 'Map';
    memberType: SchemaType & MemberType;
}

export interface NullType extends BaseType {
    type: 'Null';
}

export interface NumberType extends BaseType {
    type: 'Number';
    versionAttribute?: boolean;
}

export interface NumberSetType extends BaseType {
    type: 'NumberSet';
}

export interface StringType extends BaseType {
    type: 'String';
}

export interface StringSetType extends BaseType {
    type: 'StringSet';
}

export interface TupleType extends BaseType {
    type: 'Tuple';
    members: Array<SchemaType & MemberType>;
}

export type SchemaType =
    BinaryType |
    BinarySetType |
    BooleanType |
    CustomType<any> |
    CollectionType |
    DateType |
    DocumentType |
    HashType |
    ListType |
    MapType |
    NullType |
    NumberType |
    NumberSetType |
    StringType |
    StringSetType |
    TupleType;

export function isSchemaType(arg: any): arg is SchemaType {
    if (isBaseType(arg)) {
        switch (arg.type) {
            case 'Binary':
            case 'String':
                return isKeyableType(arg);
            case 'Custom':
                return typeof (arg as CustomType<any>).marshall === 'function'
                    && typeof (arg as CustomType<any>).unmarshall === 'function';
            case 'Document': {
                const {valueConstructor, members} = arg as DocumentType;
                if (!members || typeof members !== 'object') {
                    return false;
                }

                for (let key of Object.keys(members)) {
                    if (!isSchemaType(members[key])) {
                        return false;
                    }
                }

                return ['function', 'undefined']
                    .indexOf(typeof valueConstructor) > -1;
            } case 'List':
            case 'Map':
                return isSchemaType((arg as ListType).memberType);
            case 'Number':
                return isKeyableType(arg) && ['boolean', 'undefined']
                    .indexOf(typeof (arg as NumberType).versionAttribute) > -1;
            case 'Tuple': {
                const {members} = arg as TupleType;
                if (!Array.isArray(members)) {
                    return false;
                }

                for (let member of members) {
                    if (!isSchemaType(member)) {
                        return false;
                    }
                }

                return true;
            } default:
                return true;
        }
    }

    return false;
}