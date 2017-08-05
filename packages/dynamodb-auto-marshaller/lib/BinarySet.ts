export type BinaryValue = ArrayBuffer|ArrayBufferView;

/**
 * A set of binary values represented as either ArrayBuffer objects or
 * ArrayBufferView objects. Equality is determined by the underlying byte
 * sequence and not by the identity or view window type of the provided value.
 */
export class BinarySet implements Set<BinaryValue> {
    private _values: Array<BinaryValue> = [];

    /**
     * Creates a new BinarySet and optionally seeds it with values.
     *
     * @param iterable An optional iterable of binary values to add to the set.
     */
    constructor(iterable?: Iterable<BinaryValue>) {
        if (iterable) {
            for (let item of iterable) {
                this.add(item);
            }
        }
    }

    /**
     * Add a binary value to the set. If the value is already contained in the
     * set, it will not be added a second time.
     *
     * @param value The binary value to add
     */
    add(value: BinaryValue): this {
        if (!this.has(value)) {
            this._values.push(value);
        }

        return this;
    }

    /**
     * Remove all values from the set.
     */
    clear(): void {
        this._values = [];
    }

    /**
     * Removes a particular value from the set. If the value was contained in
     * the set prior to this method being called, `true` will be returned; if
     * the value was not in the set, `false` will be returned. In either case,
     * the value provided will not be in the set after this method returns.
     *
     * @param value The binary value to remove from the set.
     */
    delete(value: BinaryValue): boolean {
        const valueView = getBinaryView(value);
        const scrubbedValues = this._values.filter(item => {
            return !binaryEquals(getBinaryView(item), valueView);
        });

        const numRemoved = this._values.length - scrubbedValues.length;
        this._values = scrubbedValues;

        return numRemoved > 0;
    }

    /**
     * Returns an iterable two-member tuples for each item in the set, where
     * the item is provided twice.
     *
     * Part of the ES2015 Set specification for compatibility with Map objects.
     */
    entries(): IterableIterator<[BinaryValue, BinaryValue]> {
        return this._values.map<[BinaryValue, BinaryValue]>(
            value => [value, value]
        )[Symbol.iterator]();
    }

    /**
     * Invokes a callback once for each member of the set.
     *
     * @param callback The function to invoke with each set member
     * @param thisArg The `this` context on which to invoke the callback
     */
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

    /**
     * Determines if a provided value is already a member of the set.
     *
     * Equality is determined by inspecting the bytes of the ArrayBuffer or
     * ArrayBufferView.
     *
     * @example On a little-endian system, the following values would be
     * considered equal:
     *
     *     new Uint32Array([0xdeadbeef]);
     *     (new Uint32Array([0xdeadbeef])).buffer;
     *     new Uint16Array([0xbeef, 0xdead]);
     *     new Uint8Array([0xef, 0xbe, 0xad, 0xde]);
     *
     * @param value The binary value against which set members should be checked
     */
    has(value: BinaryValue): boolean {
        const valueView = getBinaryView(value);

        for (let item of this) {
            if (binaryEquals(getBinaryView(item), valueView)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns an IterableIterator of each member of the set.
     */
    keys(): IterableIterator<BinaryValue> {
        return this[Symbol.iterator]();
    }

    /**
     * Returns the number of members in the set.
     */
    get size(): number {
        return this._values.length;
    }

    /**
     * Returns an IterableIterator of each member of the set.
     */
    values(): IterableIterator<BinaryValue> {
        return this[Symbol.iterator]();
    }

    /**
     * Returns an IterableIterator of each member of the set.
     */
    [Symbol.iterator](): IterableIterator<BinaryValue> {
        return this._values[Symbol.iterator]();
    }

    get [Symbol.toStringTag](): 'Set' {
        return 'Set';
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
