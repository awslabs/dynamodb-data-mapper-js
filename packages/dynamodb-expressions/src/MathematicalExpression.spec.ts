import {AttributePath} from "./AttributePath";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {MathematicalExpression} from "./MathematicalExpression";

describe('MathematicalExpression', () => {
    const basicMathematicalExpression = new MathematicalExpression(
        new AttributePath('foo'),
        '+',
        1
    );

    describe('::isMathematicalExpression', () => {
        it('should accept valid mathematical expressions', () => {
            expect(
                MathematicalExpression
                    .isMathematicalExpression(basicMathematicalExpression)
            ).toBe(true);
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
                expect(
                    MathematicalExpression
                        .isMathematicalExpression(notMathematicalExpression)
                ).toBe(false);
            }
        });
    });

    describe('#serialize', () => {
        it('should serialize basic mathematical expressions', () => {
            const attributes = new ExpressionAttributes();
            expect(basicMathematicalExpression.serialize(attributes))
                .toBe('#attr0 + :val1');

            expect(attributes.names).toEqual({
                '#attr0': 'foo',
            });

            expect(attributes.values).toEqual({
                ':val1': {N: '1'},
            });
        });
    });
});
