import { ProvisionedThroughput } from './ProvisionedThroughput';
import { PerIndexOptions } from './SecondaryIndexOptions';

export interface CreateTableOptions extends ProvisionedThroughput {
    streamViewType?: StreamViewType;
    indexOptions?: PerIndexOptions;
    billingMode?: BillingMode;
}

export type BillingMode = 'PROVISIONED' | 'PAY_PER_REQUEST';

export type StreamViewType =
    'NEW_IMAGE' |
    'OLD_IMAGE' |
    'NEW_AND_OLD_IMAGES' |
    'KEYS_ONLY' |
    'NONE';
