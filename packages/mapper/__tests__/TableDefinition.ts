import {isTableDefinition} from "../lib/TableDefinition";

describe('isTableDefinition', () => {
    it('should accept valid table definitions', () => {
        expect(isTableDefinition({
            tableName: 'table',
            schema: {
                key: {
                    type: 'String',
                    keyConfiguration: { type: 'HASH' }
                },

            }
        })).toBe(true);
    });

    it('should reject table definitions without a tableName', () => {
        expect(isTableDefinition({
            schema: {
                key: {
                    type: 'String',
                    keyConfiguration: { type: 'HASH' }
                },

            }
        })).toBe(false);
    });

    it('should reject table definitions with an invalid schema', () => {
        expect(isTableDefinition({
            tableName: 'table',
            schema: {
                key: {
                    type: 'foo',
                },

            }
        })).toBe(false);
    });
});
