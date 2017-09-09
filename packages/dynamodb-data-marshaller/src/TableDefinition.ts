import {isSchema, Schema} from "./Schema";

/**
 * An object providing the schema and name of a DynamoDB table.
 */
export interface TableDefinition {
    tableName: string;
    schema: Schema;
}

/**
 * Evaluates whether the provided argument is a TableDefinition object
 */
export function isTableDefinition(arg: any): arg is TableDefinition {
    return Boolean(arg) && typeof arg === 'object'
        && typeof arg.tableName === 'string'
        && isSchema(arg.schema);
}
