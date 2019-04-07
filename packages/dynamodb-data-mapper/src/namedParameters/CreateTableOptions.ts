import { ProvisionedThroughput } from './ProvisionedThroughput';
import { PerIndexOptions } from './SecondaryIndexOptions';

interface BaseCreateTableOptions {
    streamViewType?: StreamViewType;
    indexOptions?: PerIndexOptions;
    billingMode?: BillingMode;
}

export interface ProvisionedCreateTableOptions extends ProvisionedThroughput, BaseCreateTableOptions {
    billingMode?: 'PROVISIONED';
}

export interface OnDemandCreateTableOptions extends BaseCreateTableOptions {
    billingMode: 'PAY_PER_REQUEST';
}

export type CreateTableOptions = ProvisionedCreateTableOptions | OnDemandCreateTableOptions;

export type BillingMode = 'PROVISIONED' | 'PAY_PER_REQUEST';

export type StreamViewType =
    'NEW_IMAGE' |
    'OLD_IMAGE' |
    'NEW_AND_OLD_IMAGES' |
    'KEYS_ONLY' |
    'NONE';
