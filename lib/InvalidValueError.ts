export class InvalidValueError extends Error {
    constructor(
        public readonly invalidValue: any,
        message?: string
    ) {
        super(message);
    }
}