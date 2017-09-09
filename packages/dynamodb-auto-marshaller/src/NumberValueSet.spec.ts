import {NumberValue} from "./NumberValue";
import {NumberValueSet} from "./NumberValueSet";

describe('NumberValueSet', () => {
    it('should create a set with values provided to the constructor', () => {
        const set = new NumberValueSet([
            new NumberValue('1'),
            new NumberValue('2'),
        ]);

        expect(set.size).toBe(2);

        expect(set.has(new NumberValue('1'))).toBe(true);
        expect(set.has(new NumberValue('2'))).toBe(true);
        expect(set.has(new NumberValue('3'))).toBe(false);
    });

    describe('#add', () => {
        it('should add new values to the set', () => {
            const set = new NumberValueSet([
                new NumberValue('1'),
                new NumberValue('2'),
            ]);
            expect(set.has(new NumberValue('3'))).toBe(false);

            set.add(new NumberValue('3'));
            expect(set.has(new NumberValue('3'))).toBe(true);
        });

        it('should be a no-op if the value is already in the set', () => {
            const set = new NumberValueSet([new NumberValue('3')]);
            expect(set.size).toBe(1);
            set.add(new NumberValue('3'));
            expect(set.size).toBe(1);
        });

        it('should allow adding number primitives', () => {
            const set = new NumberValueSet([new NumberValue('3')]);
            expect(set.size).toBe(1);

            set.add(3);
            expect(set.size).toBe(1);
            expect(set.has(3)).toBe(true);
            expect(set.has(new NumberValue('3'))).toBe(true);

            set.add(4);
            expect(set.size).toBe(2);
            expect(set.has(4)).toBe(true);
            expect(set.has(new NumberValue('4'))).toBe(true);
        });
    });

    describe('#clear', () => {
        it('should drop all values', () => {
            const set = new NumberValueSet([
                new NumberValue('1'),
                new NumberValue('2'),
            ]);
            set.clear();
            expect(set.size).toBe(0);
        });
    });

    describe('#delete', () => {
        it(
            'should return `true` and remove the provided value if it was found in the set',
            () => {
                const set = new NumberValueSet([
                    new NumberValue('1'),
                    new NumberValue('2'),
                ]);
                expect(set.delete(new NumberValue('1'))).toBe(true);
                expect(set.size).toBe(1);
                expect(set.has(new NumberValue('1'))).toBe(false);
            }
        );

        it(
            'should return false and be a no-op if the value is not in the set',
            () => {
                const set = new NumberValueSet([
                    new NumberValue('1'),
                    new NumberValue('2'),
                ]);
                expect(set.delete(new NumberValue('3'))).toBe(false);
                expect(set.size).toBe(2);
            }
        );
    });

    describe('#entries', () => {
        it(
            'should provide a [key, value] iterable where the key and value are the same (in line with ES6 Set behavior)',
            () => {
                const set = new NumberValueSet([
                    new NumberValue('1'),
                    new NumberValue('2'),
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
            const set = new NumberValueSet([
                new NumberValue('1'),
                new NumberValue('2'),
            ]);
            const otherSet = new NumberValueSet();
            set.forEach(otherSet.add, otherSet);

            expect(otherSet.size).toBe(set.size);
        });
    });

    describe('#keys', () => {
        it(
            'should iterate over all values in the set (in line with ES6 Set behavior)',
            () => {
                const set = new NumberValueSet([
                    new NumberValue('1'),
                    new NumberValue('2'),
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
            const set = new NumberValueSet([
                new NumberValue('1'),
                new NumberValue('2'),
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
            const set = new NumberValueSet([
                new NumberValue('1'),
                new NumberValue('2'),
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
            expect(new NumberValueSet()[Symbol.toStringTag]).toBe('Set');
        });

        it('should cause toString to return a Set-identifying string', () => {
            expect(Object.prototype.toString.call(new NumberValueSet()))
                .toBe('[object Set]');
        });
    });
});
