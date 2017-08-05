import {BinarySet} from "../lib/BinarySet";

describe('BinarySet', () => {
    it('should create a set with values provided to the constructor', () => {
        const set = new BinarySet([
            new Uint8Array([0xde, 0xad]),
            new Uint8Array([0xbe, 0xef]),
        ]);

        expect(set.size).toBe(2);

        expect(set.has(new Uint8Array([0xde, 0xad]))).toBe(true);
        expect(set.has(new Uint8Array([0xbe, 0xef]))).toBe(true);
        expect(set.has(new Uint8Array([0xfa, 0xce]))).toBe(false);
    });

    describe('#add', () => {
        it('should add new values to the set', () => {
            const set = new BinarySet([
                new Uint8Array([0xde, 0xad]),
                new Uint8Array([0xbe, 0xef]),
                new Uint8Array(0),
            ]);
            expect(set.has(new Uint8Array([0xfa, 0xce]))).toBe(false);

            set.add(new Uint8Array([0xfa, 0xce]));
            expect(set.has(new Uint8Array([0xfa, 0xce]))).toBe(true);
        });

        it('should be a no-op if the value is already in the set', () => {
            const set = new BinarySet([new Uint8Array(1)]);
            expect(set.size).toBe(1);
            set.add(new ArrayBuffer(1));
            expect(set.size).toBe(1);
        });
    });

    describe('#clear', () => {
        it('should drop all values', () => {
            const set = new BinarySet([
                new Uint8Array([0xde, 0xad]),
                new Uint8Array([0xbe, 0xef]),
            ]);
            set.clear();
            expect(set.size).toBe(0);
        });
    });

    describe('#delete', () => {
        it(
            'should return `true` and remove the provided value if it was found in the set',
            () => {
                const set = new BinarySet([
                    new Uint8Array([0xde, 0xad]),
                    new Uint8Array([0xbe, 0xef]),
                ]);
                expect(set.delete(new Uint8Array([0xde, 0xad]))).toBe(true);
                expect(set.size).toBe(1);
                expect(set.has(new Uint8Array([0xde, 0xad]))).toBe(false);
            }
        );

        it(
            'should remove values with the same underlying binary value even if the object is a different view type',
            () => {
                const set = new BinarySet([
                    new Uint8Array([0xde, 0xad]),
                    new Uint8Array([0xbe, 0xef]),
                ]);
                expect(set.delete(
                    new Int16Array(new Uint8Array([0xde, 0xad]).buffer)
                )).toBe(true);
                expect(set.size).toBe(1);
            }
        );

        it(
            'should return false and be a no-op if the value is not in the set',
            () => {
                const set = new BinarySet([
                    new Uint8Array([0xde, 0xad]),
                    new Uint8Array([0xbe, 0xef]),
                ]);
                expect(set.delete(new Uint8Array([0xfa, 0xce]))).toBe(false);
                expect(set.size).toBe(2);
            }
        );
    });

    describe('#entries', () => {
        it(
            'should provide a [key, value] iterable where the key and value are the same (in line with ES6 Set behavior',
            () => {
                const set = new BinarySet([
                    new Uint8Array([0xde, 0xad]),
                    new Uint8Array([0xbe, 0xef]),
                ]);
                for (let [key, value] of set.entries()) {
                    expect(key).toBe(value);
                    expect(set.has(value)).toBe(true);
                }
            }
        );
    });

    describe('#forEach', () => {
        it('should invoke a callback for each value in the set', () => {
            const set = new BinarySet([
                new Uint8Array([0xde, 0xad]),
                new Uint8Array([0xbe, 0xef]),
            ]);
            const otherSet = new BinarySet();
            set.forEach(otherSet.add, otherSet);

            expect(otherSet.size).toBe(set.size);
        });
    });

    describe('#keys', () => {
        it(
            'should iterate over all values in the set (in line with ES6 Set behavior)',
            () => {
                const set = new BinarySet([
                    new Uint8Array([0xde, 0xad]),
                    new Uint8Array([0xbe, 0xef]),
                ]);

                let iterations = 0;
                for (let key of set.keys()) {
                    expect(set.has(key)).toBe(true);
                    iterations++;
                }

                expect(iterations).toBe(set.size);
            }
        );
    });

    describe('#values', () => {
        it('should iterate over all values in the set', () => {
            const set = new BinarySet([
                new Uint8Array([0xde, 0xad]),
                new Uint8Array([0xbe, 0xef]),
            ]);

            let iterations = 0;
            for (let key of set.values()) {
                expect(set.has(key)).toBe(true);
                iterations++;
            }

            expect(iterations).toBe(set.size);
        });
    });

    describe('#[Symbol.iterator]', () => {
        it('should iterate over all values in the set', () => {
            const set = new BinarySet([
                new Uint8Array([0xde, 0xad]),
                new Uint8Array([0xbe, 0xef]),
            ]);

            let iterations = 0;
            for (let key of set) {
                expect(set.has(key)).toBe(true);
                iterations++;
            }

            expect(iterations).toBe(set.size);
        });
    });

    describe('#[Symbol.toStringTag]', () => {
        it('should return a static value of "Set"', () => {
            expect(new BinarySet()[Symbol.toStringTag]).toBe('Set');
        });

        it('should cause toString to return a Set-identifying string', () => {
            expect(Object.prototype.toString.call(new BinarySet()))
                .toBe('[object Set]');
        });
    });
});
