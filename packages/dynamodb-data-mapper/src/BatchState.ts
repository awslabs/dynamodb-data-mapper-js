import {Schema, ZeroArgumentsConstructor} from '@awslabs-community-fork/dynamodb-data-marshaller';

export interface BatchState<T> {
    [tableName: string]: {
        keyProperties: Array<string>;
        itemSchemata: {
            [identifier: string]: {
                schema: Schema;
                constructor: ZeroArgumentsConstructor<T>;
            };
        };
    };
}
