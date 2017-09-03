import {ExpressionAttributes} from "./ExpressionAttributes";
import {ProjectionExpression} from './ProjectionExpression';

describe('ProjectionExpression', () => {
    it('should allow the addition of scalar values', () => {
        const expr = new ProjectionExpression();
        for (const scalar of ['foo', 'bar', 'baz', 'quux']) {
            expr.addAttribute(scalar);
        }

        expect(expr.toString()).toBe('#attr0, #attr1, #attr2, #attr3');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr1': 'bar',
            '#attr2': 'baz',
            '#attr3': 'quux',
        });
    });

    it('should allow the addition of list index dereferences', () => {
        const expr = new ProjectionExpression();
        expr.addAttribute({
            listAttributeName: 'foo',
            index: 2,
        });

        expect(expr.toString()).toBe('#attr0[2]');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
        });
    });

    it('should allow the addition of nested attributes', () => {
        const expr = new ProjectionExpression();
        expr.addAttribute({
            mapAttributeName: 'foo',
            propertyAttributeName: 'bar',
        });

        expect(expr.toString()).toBe('#attr0.#attr1');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr1': 'bar',
        });
    });

    it(
        'should allow the nesting of complex attributes to an arbitrary depth',
        () => {
            const expr = new ProjectionExpression();
            expr.addAttribute({
                mapAttributeName: {
                    listAttributeName: {
                        mapAttributeName: {
                            listAttributeName: {
                                mapAttributeName: {
                                    listAttributeName: {
                                        mapAttributeName: 'snap',
                                        propertyAttributeName: 'foo'
                                    },
                                    index: 2,
                                },
                                propertyAttributeName: 'bar',
                            },
                            index: 3,
                        },
                        propertyAttributeName: 'baz',
                    },
                    index: 4,
                },
                propertyAttributeName: 'quux',
            });

            expect(expr.toString()).toBe('#attr0.#attr1[2].#attr2[3].#attr3[4].#attr4');
            expect(expr.attributes.names).toEqual({
                '#attr0': 'snap',
                '#attr1': 'foo',
                '#attr2': 'bar',
                '#attr3': 'baz',
                '#attr4': 'quux',
            });
        }
    );

    it('should allow the injection of an ExpressionAttributes object', () => {
        const attributes = new ExpressionAttributes();
        const expr = new ProjectionExpression({attributes});
        for (const scalar of ['foo', 'bar', 'baz', 'quux']) {
            expr.addAttribute(scalar);
        }

        expect(expr.toString()).toBe('#attr0, #attr1, #attr2, #attr3');
        expect(attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr1': 'bar',
            '#attr2': 'baz',
            '#attr3': 'quux',
        });
    });
});
