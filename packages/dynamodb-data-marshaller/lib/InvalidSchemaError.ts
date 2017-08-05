import {SchemaType} from "./SchemaType";

/**
 * An error thrown when a marshaller or unmarshaller cannot understand a node of
 * the provided schema.
 */
export class InvalidSchemaError extends Error {
    constructor(public readonly node: SchemaType, message?: string) {
        super(message);
    }
}