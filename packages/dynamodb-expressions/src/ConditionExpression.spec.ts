import {
    equals,
    notEquals,
    lessThan,
    lessThanOrEqualTo,
    greaterThan,
    greaterThanOrEqualTo,
    between,
    inList,
    isConditionExpressionPredicate,
    serializeConditionExpression,
} from "./ConditionExpression";
import {ExpressionAttributes} from "./ExpressionAttributes";
import {AttributePath} from "./AttributePath";

describe('equals', () => {
    it('should return an equality condition predicate', () => {
        const pred = equals(new AttributePath('foo'));
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred.type).toBe('Equals');
    });
});

describe('notEquals', () => {
    it('should return an inequality condition predicate', () => {
        const pred = notEquals(new AttributePath('foo'));
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred.type).toBe('NotEquals');
    });
});

describe('lessThan', () => {
    it('should return an < condition predicate', () => {
        const pred = lessThan(new AttributePath('foo'));
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred.type).toBe('LessThan');
    });
});

describe('lessThanOrEqualTo', () => {
    it('should return an <= condition predicate', () => {
        const pred = lessThanOrEqualTo(new AttributePath('foo'));
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred.type).toBe('LessThanOrEqualTo');
    });
});

describe('greaterThan', () => {
    it('should return an > condition predicate', () => {
        const pred = greaterThan(new AttributePath('foo'));
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred.type).toBe('GreaterThan');
    });
});

describe('greaterThanOrEqualTo', () => {
    it('should return an >= condition predicate', () => {
        const pred = greaterThanOrEqualTo(new AttributePath('foo'));
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred.type).toBe('GreaterThanOrEqualTo');
    });
});

describe('between', () => {
    it('should return a bounded condition predicate', () => {
        const pred = between(1, 10);
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred).toEqual({
            type: 'Between',
            lowerBound: 1,
            upperBound: 10,
        });
    });
});

describe('inList', () => {
    it('should return a membership condition predicate', () => {
        const pred = inList('foo', 'bar', 'baz', 'quux');
        expect(isConditionExpressionPredicate(pred)).toBe(true);
        expect(pred).toEqual({
            type: 'Membership',
            values: [
                'foo',
                'bar',
                'baz',
                'quux',
            ]
        });
    });
});

describe('serializeConditionExpression', () => {
    it('should serialize equality expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'Equals',
                subject: 'foo',
                object: 'bar',
            },
            attributes
        );

        expect(serialized).toBe('#attr0 = :val1');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({':val1': {S: 'bar'}});
    });

    it('should serialize inequality expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {type: 'NotEquals', subject: 'foo', object: 'bar'},
            attributes
        );

        expect(serialized).toBe('#attr0 <> :val1');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({':val1': {S: 'bar'}});
    });

    it('should serialize less than expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {type: 'LessThan', subject: 'foo', object: 'bar'},
            attributes
        );

        expect(serialized).toBe('#attr0 < :val1');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({':val1': {S: 'bar'}});
    });

    it('should serialize greater than expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'GreaterThan',
                subject: 'foo',
                object: 'bar',
            },
            attributes
        );

        expect(serialized).toBe('#attr0 > :val1');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({':val1': {S: 'bar'}});
    });

    it('should serialize less than or equal to expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'LessThanOrEqualTo',
                subject: 'foo',
                object: 'bar',
            },
            attributes
        );

        expect(serialized).toBe('#attr0 <= :val1');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({':val1': {S: 'bar'}});
    });

    it('should serialize greater than or equal to expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'GreaterThanOrEqualTo',
                subject: 'foo',
                object: new AttributePath('bar'),
            },
            attributes
        );

        expect(serialized).toBe('#attr0 >= #attr1');
        expect(attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr1': 'bar',
        });
        expect(attributes.values).toEqual({});
    });

    it('should serialize bounding expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'Between',
                subject: 'foo',
                lowerBound: 1,
                upperBound: 10,
            },
            attributes
        );

        expect(serialized).toBe('#attr0 BETWEEN :val1 AND :val2');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({
            ':val1': {N: '1'},
            ':val2': {N: '10'},
        });
    });

    it('should serialize membership expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'Membership',
                subject: 'foo',
                values: [
                    1,
                    10,
                    100,
                ],
            },
            attributes
        );

        expect(serialized).toBe('#attr0 IN (:val1, :val2, :val3)');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({
            ':val1': {N: '1'},
            ':val2': {N: '10'},
            ':val3': {N: '100'},
        });
    });

    it('should serialize negation expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'Not',
                condition: {
                    type: 'Between',
                    subject: 'foo',
                    lowerBound: 1,
                    upperBound: 10,
                }
            },
            attributes
        );

        expect(serialized).toBe('NOT (#attr0 BETWEEN :val1 AND :val2)');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({
            ':val1': {N: '1'},
            ':val2': {N: '10'},
        });
    });

    it('should serialize and expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'And',
                conditions: [
                    {
                        type: 'GreaterThanOrEqualTo',
                        subject: 'foo',
                        object: 1,
                    },
                    {
                        type: 'LessThan',
                        subject: 'foo',
                        object: 10,
                    },
                    {
                        type: 'Equals',
                        subject: 'fizz',
                        object: 'buzz',
                    }
                ]
            },
            attributes
        );

        expect(serialized).toBe('(#attr0 >= :val1) AND (#attr0 < :val2) AND (#attr3 = :val4)');
        expect(attributes.names).toEqual({
            '#attr0': 'foo',
            '#attr3': 'fizz',
        });
        expect(attributes.values).toEqual({
            ':val1': {N: '1'},
            ':val2': {N: '10'},
            ':val4': {S: 'buzz'},
        });
    });

    it('should serialize or expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                type: 'Or',
                conditions: [
                    {
                        type: 'GreaterThanOrEqualTo',
                        subject: 'foo',
                        object: 10,
                    },
                    {
                        type: 'LessThan',
                        subject: 'foo',
                        object: 1,
                    }
                ]
            },
            attributes
        );

        expect(serialized).toBe('(#attr0 >= :val1) OR (#attr0 < :val2)');
        expect(attributes.names).toEqual({
            '#attr0': 'foo',
        });
        expect(attributes.values).toEqual({
            ':val1': {N: '10'},
            ':val2': {N: '1'},
        });
    });

    it('should serialize function expressions', () => {
        const attributes = new ExpressionAttributes();
        const serialized = serializeConditionExpression(
            {
                name: 'attribute_type',
                arguments: [
                    new AttributePath('foo'),
                    'S'
                ]
            },
            attributes
        );

        expect(serialized).toBe('attribute_type(#attr0, :val1)');
        expect(attributes.names).toEqual({'#attr0': 'foo'});
        expect(attributes.values).toEqual({':val1': {S: 'S'}});
    });
});
