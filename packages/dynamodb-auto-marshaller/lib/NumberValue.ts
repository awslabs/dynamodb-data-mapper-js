export class NumberValue {
    public readonly value: string;

    constructor(value: string|number) {
        this.value = value.toString().trim();
    }

    toJSON(): number {
        return this.toNumber();
    }

    toNumber(): number {
        return Number(this.value);
    }

    toString(): string {
        return this.value;
    }
}