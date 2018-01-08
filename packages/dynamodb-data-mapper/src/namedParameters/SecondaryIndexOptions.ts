import { ProvisionedThroughput } from './ProvisionedThroughput';

export type SecondaryIndexProjection = 'all'|'keys'|Array<string>;

export interface SharedSecondaryIndexOptions {
    projection: SecondaryIndexProjection;
}

export interface GlobalSecondaryIndexOptions extends
    SharedSecondaryIndexOptions,
    ProvisionedThroughput
{
    type: 'global';
}

export interface LocalSecondaryIndexOptions extends
    SharedSecondaryIndexOptions
{
    type: 'local';
}

export type SecondaryIndexOptions
    = GlobalSecondaryIndexOptions | LocalSecondaryIndexOptions;

export interface PerIndexOptions {
    [indexName: string]: SecondaryIndexOptions;
}
