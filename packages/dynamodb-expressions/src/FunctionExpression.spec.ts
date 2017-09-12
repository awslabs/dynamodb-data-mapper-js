import {
    FunctionExpression,
    isFunctionExpression,
    serializeFunctionExpression,
} from "./FunctionExpression";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

describe('FunctionExpression', () => {
    const basicFunctionExpression: FunctionExpression = {
        name: 'foo',
        arguments: [
            new AttributePath('bar'),
            'baz',
        ],
    };

    describe('isFunctionExpression', () => {
        it('should accept valid function expressions', () => {
            expect(isFunctionExpression(basicFunctionExpression)).toBe(true);
        });

        it('should reject non-matching values', () => {
            for (const notFunctionExpression of [
                false,
                true,
                null,
                void 0,
                'string',
                123,
                [],
                {},
                new Uint8Array(12),
                {foo: 'bar'},
                {name: 'foo', arguments: 'bar'},
            ]) {
                expect(isFunctionExpression(notFunctionExpression)).toBe(false);
            }
        });
    });

    describe('serializeFunctionExpression', () => {
        it('should serialize basic function expressions', () => {
            const attributes = new ExpressionAttributes();
            expect(
                serializeFunctionExpression(basicFunctionExpression, attributes)
            ).toBe('foo(#attr0, :val1)');

            expect(attributes.names).toEqual({
                '#attr0': 'bar',
            });

            expect(attributes.values).toEqual({
                ':val1': {S: 'baz'},
            });
        });
    });
});
