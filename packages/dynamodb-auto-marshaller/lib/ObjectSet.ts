export abstract class ObjectSet<T> implements Set<T> {
    protected _values: Array<T> = [];

    /**
     * Creates a new ObjectSet and optionally seeds it with values.
     *
     * @param iterable An optional iterable of values to add to the set.
     */
    constructor(iterable?: Iterable<T>) {
        if (iterable) {
            for (let item of iterable) {
                this.add(item);
            }
        }
    }

    /**
     * Add a value to the set. If the value is already contained in the set, it
     * will not be added a second time.
     *
     * @param value The value to add
     */
    add(value: T): this {
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
     * @param value The value to remove from the set.
     */
    abstract delete(value: T): boolean;

    /**
     * Returns an iterable two-member tuples for each item in the set, where
     * the item is provided twice.
     *
     * Part of the ES2015 Set specification for compatibility with Map objects.
     */
    entries(): IterableIterator<[T, T]> {
        return this._values.map<[T, T]>(
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
            value: T,
            value2: T,
            set: Set<T>
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
     * @param value The value against which set members should be checked
     */
    abstract has(value: T): boolean;

    /**
     * Returns an IterableIterator of each member of the set.
     */
    keys(): IterableIterator<T> {
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
    values(): IterableIterator<T> {
        return this[Symbol.iterator]();
    }

    /**
     * Returns an IterableIterator of each member of the set.
     */
    [Symbol.iterator](): IterableIterator<T> {
        return this._values[Symbol.iterator]();
    }

    /**
     * Returns the string literal 'Set' for use by Object.prototype.toString.
     * This allows for identifying Sets without checking constructor identity.
     */
    get [Symbol.toStringTag](): 'Set' {
        return 'Set';
    }
}