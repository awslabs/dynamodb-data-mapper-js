import {NumberValue} from "./NumberValue";

describe('NumberValue', function() {
    it('should store numbers', function() {
        const number = new NumberValue(123);
        expect(number.toString()).toBe('123');
    });

    it('should store numeric strings', function() {
        const number = new NumberValue('123.1');
        expect(number.toString()).toBe('123.1');
    });

    it(
        'should store numeric values that would lose precision if converted to JavaScript numbers',
        function() {
            const number = new NumberValue('900719925474099100');
            if (typeof (Number as any).isSafeInteger === 'function') {
                expect((Number as any).isSafeInteger(number.valueOf()))
                    .toBe(false);
            }
            expect(number.toString()).toBe('900719925474099100');
        }
    );

    it('should convert numeric strings to numbers', function() {
        const number = new NumberValue('123.1');
        expect(number.valueOf()).toBe(123.1);
    });

    it('should allow easy conversion of the value into a number', () => {
        const safeNum = new NumberValue('123');
        expect(+safeNum).toBe(123);
        expect((safeNum as any) + 1).toBe(124);
    });

    it('should appear as a numeric value when converted to JSON', function() {
        expect(JSON.stringify({
            number: new NumberValue('123'),
            nested: {
                number: new NumberValue('234')
            }
        })).toBe('{"number":123,"nested":{"number":234}}');
    });

    it(
        'should reply to Object.prototype.toString with [object DynamoDbNumberValue]',
        () => {
            const number = new NumberValue('900719925474099100');
            expect(Object.prototype.toString.call(number))
                .toBe('[object DynamoDbNumberValue]');
        }
    );

    describe('::isNumberValue', () => {
        it('should return `true` for NumberValue objects', () => {
            expect(NumberValue.isNumberValue(new NumberValue('0'))).toBe(true);
        });

        it('should return `false` for other values', () => {
            for (const invalid of [
                'string',
                123,
                null,
                void 0,
                true,
                [],
                {},
                new Uint8Array(10)]
            ) {
                expect(NumberValue.isNumberValue(invalid)).toBe(false);
            }
        });
    });
});
