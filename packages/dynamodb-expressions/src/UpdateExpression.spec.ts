import {UpdateExpression} from "./UpdateExpression";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

describe('UpdateExpression', () => {
    it('should serialize ADD clauses', () => {
        const expr = new UpdateExpression();
        expr.add('foo', new Set(['bar', 'baz']));
        expr.add('fizz', 1);

        expect(expr.toString()).toBe('ADD #attr0 :val1, #attr2 :val3');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr2': 'fizz',
        });
        expect(expr.attributes.values).toEqual({
            ':val1': {SS: ['bar', 'baz']},
            ':val3': {N: '1'},
        });
    });

    it('should serialize DELETE clauses', () => {
        const expr = new UpdateExpression();
        expr.delete('foo', new Set(['bar', 'baz']));
        expr.delete('fizz', 1);

        expect(expr.toString()).toBe('DELETE #attr0 :val1, #attr2 :val3');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr2': 'fizz',
        });
        expect(expr.attributes.values).toEqual({
            ':val1': {SS: ['bar', 'baz']},
            ':val3': {N: '1'},
        });
    });

    it('should serialize REMOVE clauses', () => {
        const expr = new UpdateExpression();
        expr.remove('foo');
        expr.remove('fizz');

        expect(expr.toString()).toBe('REMOVE #attr0, #attr1');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr1': 'fizz',
        });
        expect(expr.attributes.values).toEqual({});
    });

    it('should serialize SET clauses', () => {
        const expr = new UpdateExpression();
        expr.set('foo', new Set(['bar', 'baz']));
        expr.set('fizz', 1);

        expect(expr.toString()).toBe('SET #attr0 = :val1, #attr2 = :val3');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr2': 'fizz',
        });
        expect(expr.attributes.values).toEqual({
            ':val1': {SS: ['bar', 'baz']},
            ':val3': {N: '1'},
        });
    });

    it('should serialize SET clauses with function expressions', () => {
        const expr = new UpdateExpression();
        expr.set('foo', {
            name: 'list_append',
            arguments: [
                new AttributePath('foo'),
                'bar'
            ]
        });

        expect(expr.toString()).toBe('SET #attr0 = list_append(#attr0, :val1)');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
        });
        expect(expr.attributes.values).toEqual({
            ':val1': {S: 'bar'},
        });
    });

    it('should serialize SET clauses with mathematical expressions', () => {
        const expr = new UpdateExpression();
        expr.set('foo', {
            leftHandSide: new AttributePath('foo'),
            operator: '+',
            rightHandSide: 1
        });

        expect(expr.toString()).toBe('SET #attr0 = #attr0 + :val1');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
        });
        expect(expr.attributes.values).toEqual({
            ':val1': {N: '1'},
        });
    });

    it('should serialize expressions with multiple clauses', () => {
        const expr = new UpdateExpression();
        expr.add('foo', new Set(['baz']));
        expr.delete('foo', new Set(['quux']));
        expr.remove('fizz');
        expr.set('buzz', new Set(['pop']));

        expect(expr.toString()).toBe('ADD #attr0 :val1 DELETE #attr0 :val2 SET #attr4 = :val5 REMOVE #attr3');
        expect(expr.attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr3': 'fizz',
            '#attr4': 'buzz',
        });
        expect(expr.attributes.values).toEqual({
            ':val1': {SS: ['baz']},
            ':val2': {SS: ['quux']},
            ':val5': {SS: ['pop']},
        });
    });

    it('should support injecting an instance of ExpressionAttributes', () => {
        const attributes = new ExpressionAttributes();
        const expr = new UpdateExpression({attributes});
        expr.remove('foo');

        expect(attributes.names).toEqual({'#attr0': 'foo'});
    });
});
