import {
    isMathematicalExpression,
    MathematicalExpression,
    serializeMathematicalExpression,
} from "./MathematicalExpression";
import {ExpressionAttributes} from "./ExpressionAttributes";

describe('MathematicalExpression', () => {
    const basicMathematicalExpression: MathematicalExpression = {
        leftHandSide: 'foo',
        operator: '+',
        rightHandSide: 1,
    };

    describe('isMathematicalExpression', () => {
        it('should accept valid mathematical expressions', () => {
            expect(isMathematicalExpression(basicMathematicalExpression))
                .toBe(true);
        });

        it('should reject non-matching values', () => {
            for (const notMathematicalExpression of [
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
            ]) {
                expect(isMathematicalExpression(notMathematicalExpression))
                    .toBe(false);
            }
        });
    });

    describe('serializeMathematicalExpression', () => {
        it('should serialize basic mathematical expressions', () => {
            const attributes = new ExpressionAttributes();
            expect(serializeMathematicalExpression(
                basicMathematicalExpression,
                attributes
            )).toBe('#attr0 + :val1');

            expect(attributes.names).toEqual({
                '#attr0': 'foo',
            });

            expect(attributes.values).toEqual({
                ':val1': {N: '1'},
            });
        });
    });
});
