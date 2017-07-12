import {isSchema, Schema} from "./Schema";

export interface TableDefinition {
    tableName: string;
    schema: Schema;
}

export function isTableDefinition(arg: any): arg is TableDefinition {
    return Boolean(arg) && typeof arg === 'object'
        && typeof arg.tableName === 'string'
        && isSchema(arg.schema);
}
