import {NumberValue} from "../lib/NumberValue";

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
                expect((Number as any).isSafeInteger(number.toNumber()))
                    .toBe(false);
            }
            expect(number.toString()).toBe('900719925474099100');
        }
    );

    it('should convert numeric strings to numbers', function() {
        const number = new NumberValue('123.1');
        expect(number.toNumber()).toBe(123.1);
    });


    it('should appear as a numeric value when converted to JSON', function() {
        expect(JSON.stringify({
            number: new NumberValue('123'),
            nested: {
                number: new NumberValue('234')
            }
        })).toBe('{"number":123,"nested":{"number":234}}');
    });
});