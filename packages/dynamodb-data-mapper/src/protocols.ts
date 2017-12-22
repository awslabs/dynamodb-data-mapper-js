import {Schema} from '@aws/dynamodb-data-marshaller';

/**
 * Table metadata is reported by items submitted to the data mapper via methods
 * identified by symbols. This is done both to disambiguate data (which should
 * always be identified by string keys) from metadata and also to allow an
 * eventually integration with the First-Class Protocols proposal as described
 * at {@link https://github.com/michaelficarra/proposal-first-class-protocols}
 * (currently at stage 1 in the ECMAScript change acceptance process).
 *
 * Because the protocol proposal allows implementation to be declared
 * dynamically at runtime (and also because TypeScript does not allow
 * user-defined symbols to appear in type declarations), protocol adherence
 * should be detected on objects at runtime rather than on types via static
 * analysis.
 */

/**
 * Used to designate the mapping of an object from its JavaScript form to its
 * representation in a DynamoDB Table or nested map.
 *
 * @example
 *
 *      class FooDocument {
 *          [DynamoDbSchema]() {
 *              return {
 *                  bar: {type: 'String'},
 *                  baz: {type: 'Number'},
 *              };
 *          }
 *      }
 */
export const DynamoDbSchema = Symbol('DynamoDbSchema');

export function getSchema(item: any): Schema {
    if (item) {
        const schema = item[DynamoDbSchema];
        if (schema && typeof schema === 'object') {
            return schema;
        }
    }

    throw new Error(
        'The provided item did not adhere to the DynamoDbDocument protocol.' +
        ' No object property was found at the `DynamoDbSchema` symbol'
    );
}

/**
 * Used to designate that an object represents a row of the named DynamoDB
 * table. Meant to be used in conjunction with {DynamoDbSchema}.
 *
 * @example
 *
 *      class FooDocument {
 *          [DynamoDbTable]() {
 *              return 'FooTable';
 *          }
 *
 *          [DynamoDbSchema]() {
 *              return {
 *                  bar: {type: 'String'},
 *                  baz: {type: 'Number'},
 *              };
 *          }
 *      }
 */
export const DynamoDbTable = Symbol('DynamoDbTableName');

export function getTableName(item: any, tableNamePrefix: string = ''): string {
    if (item) {
        const tableName = item[DynamoDbTable];
        if (typeof tableName === 'string') {
            return tableNamePrefix + tableName;
        }
    }

    throw new Error(
        'The provided item did not adhere to the DynamoDbTable protocol. No' +
        ' string property was found at the `DynamoDbTable` symbol'
    );
}

/**
 * Used to designate which fields on an object have been changed. The method
 * identified by this symbol should return a iterable that enumerates the fields
 * that have been altered.
 *
 * @example
 *
 *      class FooDocument {
 *          constructor() {
 *              this._dirtyFields = new Set();
 *              this._foo = '';
 *          }
 *
 *          get foo() {
 *              return this._foo;
 *          }
 *
 *          set foo(value) {
 *              this._foo = value;
 *              this._dirtyFields.add('foo');
 *          }
 *
 *          [DynamoDbDirtyFields]() {
 *              return this._dirtyFields.values();
 *          }
 *      }
 */
export const DynamoDbDirtyFields = Symbol('DynamoDbDirtyFields');
