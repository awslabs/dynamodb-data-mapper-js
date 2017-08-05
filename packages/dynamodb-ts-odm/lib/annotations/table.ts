import {ZeroArgumentsConstructor} from "../../../dynamodb-data-marshaller/lib/SchemaType";
import 'reflect-metadata';

export function table<T>(constructor: ZeroArgumentsConstructor<T>): void {
    const md = Reflect.getMetadata('design:type', constructor, 'title');
    console.log('cat');
}
