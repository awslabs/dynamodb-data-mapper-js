import 'reflect-metadata';
import {METADATA_TYPE_KEY} from './constants';
import {BinarySet, NumberValueSet} from "@aws/dynamodb-auto-marshaller";
import {DynamoDbSchema} from '@aws/dynamodb-data-mapper';
import {
    DocumentType,
    Schema,
    SchemaType,
    SetType
} from "@aws/dynamodb-data-marshaller";

/**
 * Declare a property in a TypeScript class to be part of a DynamoDB schema.
 * Meant to be used as a property decorator in conjunction with TypeScript's
 * emitted type metadata. If used with in a project compiled with the
 * `emitDecoratorMetadata` option enabled, the type will infer most types from
 * the TypeScript source.
 *
 * Please note that TypeScript does not emit any metadata about the type
 * parameters supplied to generic types, so `Array<string>`, `[number, string]`,
 * and `MyClass[]` are all exposed as `Array` via the emitted metadata. Without
 * additional metadata, this annotation will treat all encountered arrays as
 * collections of untyped data. You may supply either a `members` declaration or
 * a `memberType` declaration to direct this annotation to treat a property as a
 * tuple or typed list, respectively.
 *
 * Member type declarations are required for maps and sets.
 *
 * @see https://www.typescriptlang.org/docs/handbook/decorators.html
 * @see https://www.typescriptlang.org/docs/handbook/compiler-options.html
 * @see https://github.com/Microsoft/TypeScript/issues/2577
 *
 * @example
 *  export class MyClass {
 *      @attribute()
 *      id: string;
 *
 *      @attribute()
 *      subdocument?: MyOtherClass;
 *
 *      @attribute()
 *      untypedCollection?: Array<any>;
 *
 *      @attribute({memberType: {type: 'String'}})
 *      listOfStrings?: Array<string>;
 *
 *      @attribute({members: [{type: 'Boolean', type: 'String'}]})
 *      tuple?: [boolean, string];
 *
 *      @attribute({memberType: {type: 'String'}})
 *      mapStringString?: Map<string, string>;
 *
 *      @attribute()
 *      binary?: Uint8Array;
 *  }
 */
export function attribute(parameters: Partial<SchemaType> = {}) {
    return (target: Object, propertyKey: string|symbol): void => {
        if (!Object.prototype.hasOwnProperty.call(target, DynamoDbSchema)) {
            Object.defineProperty(
                target,
                DynamoDbSchema as any, // TypeScript complains about the use of symbols here, though it should be allowed
                {value: deriveBaseSchema(target)}
            );
        }

        (target as any)[DynamoDbSchema][propertyKey] = metadataToSchemaType(
            Reflect.getMetadata(METADATA_TYPE_KEY, target, propertyKey),
            parameters
        );
    };
}

function deriveBaseSchema(target: any): Schema {
    if (target && typeof target === 'object') {
        const prototype = Object.getPrototypeOf(target);
        if (prototype) {
            return {
                ...deriveBaseSchema(prototype),
                ...Object.prototype.hasOwnProperty.call(prototype, DynamoDbSchema)
                    ? prototype[DynamoDbSchema]
                    : {}
            };
        }
    }

    return {};
}

function metadataToSchemaType(
    ctor: {new (): any}|undefined,
    declaration: Partial<SchemaType>
): SchemaType {
    let {type, ...rest} = declaration;
    if (type === undefined) {
        if (ctor) {
            if (ctor === String) {
                type = 'String';
            } else if (ctor === Number) {
                type = 'Number';
            } else if (ctor === Boolean) {
                type = 'Boolean';
            } else if (ctor === Date || ctor.prototype instanceof Date) {
                type = 'Date';
            } else if (
                ctor === BinarySet ||
                ctor.prototype instanceof BinarySet
            ) {
                type = 'Set';
                (rest as SetType).memberType = 'Binary';
            } else if (
                ctor === NumberValueSet ||
                ctor.prototype instanceof NumberValueSet
            ) {
                type = 'Set';
                (rest as SetType).memberType = 'Number';
            } else if (ctor === Set || ctor.prototype instanceof Set) {
                type = 'Set';
                if (!('memberType' in rest)) {
                    throw new Error(
                        'Invalid set declaration. You must specify a memberType'
                    );
                }
            } else if (ctor === Map || ctor.prototype instanceof Map) {
                type = 'Map';
                if (!('memberType' in rest)) {
                    throw new Error(
                        'Invalid map declaration. You must specify a memberType'
                    );
                }
            } else if (ctor.prototype[DynamoDbSchema]) {
                type = 'Document';
                (rest as DocumentType).members = ctor.prototype[DynamoDbSchema];
                (rest as DocumentType).valueConstructor = ctor;
            } else if (isBinaryType(ctor)) {
                type = 'Binary';
            } else if (ctor === Array || ctor.prototype instanceof Array) {
                if ('members' in declaration) {
                    type = 'Tuple';
                } else if ('memberType' in declaration) {
                    type = 'List';
                } else {
                    type = 'Collection';
                }
            } else {
                type = 'Any';
            }
        } else {
            type = 'Any';
        }
    }

    return {
        ...rest,
        type
    } as SchemaType;
}

/**
 * ArrayBuffer.isView will only evaluate if an object instance is an
 * ArrayBufferView, but TypeScript metadata gives us a reference to the class.
 *
 * This function checks if the provided constructor is or extends the built-in
 * `ArrayBuffer` constructor, the `DataView` constructor, or any `TypedArray`
 * constructor.
 *
 * This function will need to be modified if new binary types are added to
 * JavaScript (e.g., the `Int64Array` or `Uint64Array` discussed in
 * {@link https://github.com/tc39/proposal-bigint the BigInt TC39 proposal}.
 *
 * @see https://developer.mozilla.org/en-US/docs/Web/API/ArrayBufferView
 */
function isBinaryType(arg: any): boolean {
    return arg === Uint8Array || arg.prototype instanceof Uint8Array ||
        arg === Uint8ClampedArray || arg.prototype instanceof Uint8ClampedArray ||
        arg === Uint16Array || arg.prototype instanceof Uint16Array ||
        arg === Uint32Array || arg.prototype instanceof Uint32Array ||
        arg === Int8Array || arg.prototype instanceof Int8Array ||
        arg === Int16Array || arg.prototype instanceof Int16Array ||
        arg === Int32Array || arg.prototype instanceof Int32Array ||
        arg === Float32Array || arg.prototype instanceof Float32Array ||
        arg === Float64Array || arg.prototype instanceof Float64Array ||
        arg === ArrayBuffer || arg.prototype instanceof ArrayBuffer ||
        arg === DataView || arg.prototype instanceof DataView;
}
