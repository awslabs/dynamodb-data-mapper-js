const NUMBER_VALUE_TAG = 'DynamoDbNumberValue';
const EXPECTED_TAG = `[object ${NUMBER_VALUE_TAG}]`;

export class NumberValue {
    public readonly value: string;

    constructor(value: string|number) {
        this.value = value.toString().trim();
    }

    toJSON(): number {
        return this.valueOf();
    }

    toString(): string {
        return this.value;
    }

    valueOf(): number {
        return Number(this.value);
    }

    get [Symbol.toStringTag](): string {
        return NUMBER_VALUE_TAG;
    }

    static isNumberValue(arg: any): arg is NumberValue {
        return Boolean(arg)
            && Object.prototype.toString.call(arg) === EXPECTED_TAG;
    }
}
