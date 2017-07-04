/**
 * An error thrown by the marshaller when a node of the provided input cannot be
 * marshalled into the type specified in the schema.
 */
export class InvalidValueError extends Error {
    constructor(
        public readonly invalidValue: any,
        message?: string
    ) {
        super(message);
    }
}