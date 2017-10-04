import {DynamoDbTable} from '@aws/dynamodb-data-mapper';
import {ZeroArgumentsConstructor} from "@aws/dynamodb-data-marshaller";

/**
 * Declare a TypeScript class to be represent items in a table in a way
 * understandable by the AWS DynamoDB DataMapper for JavaScript. Meant to be
 * used as a TypeScript class decorator in projects compiled with the
 * `experimentalDecorators` option enabled.
 *
 * @see https://www.typescriptlang.org/docs/handbook/decorators.html
 * @see https://www.typescriptlang.org/docs/handbook/compiler-options.html
 */
export function table(tableName: string) {
    return (constructor: ZeroArgumentsConstructor<any>) => {
        constructor.prototype[DynamoDbTable] = tableName;
    };
}
