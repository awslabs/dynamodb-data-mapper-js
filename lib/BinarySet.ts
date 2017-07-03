export type BinaryValue = ArrayBuffer|ArrayBufferView;

export class BinarySet implements Set<BinaryValue> {
    private _values: Array<BinaryValue> = [];

    constructor(iterable?: Iterable<BinaryValue>) {
        if (iterable) {
            for (let item of iterable) {
                this.add(item);
            }
        }
    }

    add(value: BinaryValue): this {
        if (!this.has(value)) {
            this._values.push(value);
        }

        return this;
    }

    clear(): void {
        this._values = [];
    }

    delete(value: BinaryValue): boolean {
        const valueView = getBinaryView(value);
        const scrubbedValues = this._values.filter(item => {
            return !binaryEquals(getBinaryView(item), valueView);
        });

        const numRemoved = this._values.length - scrubbedValues.length;
        this._values = scrubbedValues;

        return numRemoved > 0;
    }

    entries(): IterableIterator<[BinaryValue, BinaryValue]> {
        return this._values.map<[BinaryValue, BinaryValue]>(
            value => [value, value]
        )[Symbol.iterator]();
    }

    forEach(
        callback: (
            value: BinaryValue,
            value2: BinaryValue,
            set: BinarySet
        ) => void,
        thisArg?: any
    ): void {
        this._values.forEach((value, index, array) => {
            callback.call(thisArg, value, value, this);
        }, thisArg);
    }

    has(value: BinaryValue): boolean {
        const valueView = getBinaryView(value);

        for (let item of this) {
            if (binaryEquals(getBinaryView(item), valueView)) {
                return true;
            }
        }

        return false;
    }

    keys(): IterableIterator<BinaryValue> {
        return this._values[Symbol.iterator]();
    }

    get size(): number {
        return this._values.length;
    }

    values(): IterableIterator<BinaryValue> {
        return this._values[Symbol.iterator]();
    }

    [Symbol.iterator](): IterableIterator<BinaryValue> {
        return this._values[Symbol.iterator]();
    }
}

function binaryEquals(a: DataView, b: DataView): boolean {
    if (a.byteLength !== b.byteLength) {
        return false;
    }

    for (let i = 0; i < a.byteLength; i++) {
        if (a.getUint8(i) !== b.getUint8(i)) {
            return false;
        }
    }

    return true;
}

function getBinaryView(value: BinaryValue): DataView {
    return ArrayBuffer.isView(value)
        ? new DataView(value.buffer, value.byteOffset, value.byteLength)
        : new DataView(value);
}