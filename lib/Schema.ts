import {isSchemaType, SchemaType} from "./SchemaType";

export interface Schema {
    [key: string]: SchemaType;
}

export function isSchema(arg: any): arg is Schema {
    if (!Boolean(arg) || typeof arg !== 'object') {
        return false;
    }

    for (let key of Object.keys(arg)) {
        if (!isSchemaType(arg[key])) {
            return false;
        }
    }

    return true;
}