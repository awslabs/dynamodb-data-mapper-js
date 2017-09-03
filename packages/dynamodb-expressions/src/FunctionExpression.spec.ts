import {
    FunctionExpression,
    isFunctionExpression,
    serializeFunctionExpression,
} from "./FunctionExpression";
import {ExpressionAttributes} from "./ExpressionAttributes";

describe('FunctionExpression', () => {
    const basicFunctionExpression: FunctionExpression = {
        name: 'foo',
        arguments: [
            'bar',
            {'S': 'baz'},
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
                {name: 'foo', arguments: ['bar', void 0]}
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
            ).toBe('foo(#attr0, :val1)')
        });
    });
});
