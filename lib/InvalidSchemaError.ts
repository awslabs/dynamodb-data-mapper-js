import {SchemaType} from "./SchemaType";

export class InvalidSchemaError extends Error {
    constructor(public readonly node: SchemaType, message?: string) {
        super(message);
    }
}