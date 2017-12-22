import {
    DynamoDbSchema,
    DynamoDbTable,
    getSchema,
    getTableName,
} from './protocols';

describe('getSchema', () => {
    it('should return the schema bound at the DynamoDbSchema symbol', () => {
        const schema = {};
        expect(getSchema({[DynamoDbSchema]: schema})).toBe(schema);
    });

    it('should throw if the provided object does not have a schema', () => {
        expect(() => getSchema({})).toThrow();
    });
});

describe('getTableName', () => {
    it('should return the name bound at the DynamoDbTable symbol', () => {
        expect(getTableName({[DynamoDbTable]: 'foo'})).toBe('foo');
    });

    it('should throw if the provided object does not have a table name', () => {
        expect(() => getTableName({})).toThrow();
    });
});
