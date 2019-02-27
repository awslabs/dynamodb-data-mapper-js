import { ProvisionedThroughput } from './ProvisionedThroughput';
import { PerIndexOptions } from './SecondaryIndexOptions';

interface BaseCreateTableOptions {
    streamViewType?: StreamViewType;
    indexOptions?: PerIndexOptions;
    billingMode?: BillingMode;
    sseSpecification?: SseSpecification;
}

export interface SseSpecification {
    sseType: SseType;
    kmsMasterKeyId?: string;
}

export interface ProvisionedCreateTableOptions extends ProvisionedThroughput, BaseCreateTableOptions {
    billingMode?: 'PROVISIONED';
}

export interface OnDemandCreateTableOptions extends BaseCreateTableOptions {
    billingMode: 'PAY_PER_REQUEST';
}

export type CreateTableOptions = ProvisionedCreateTableOptions | OnDemandCreateTableOptions;

export type BillingMode = 'PROVISIONED' | 'PAY_PER_REQUEST';

/**
 * Server-side encryption type:
 *   AES256 - Server-side encryption which uses the AES256 algorithm (not applicable).
 *   KMS - Server-side encryption which uses AWS Key Management Service.
 */
export type SseType = 'AES256' | 'KMS';

export type StreamViewType =
    'NEW_IMAGE' |
    'OLD_IMAGE' |
    'NEW_AND_OLD_IMAGES' |
    'KEYS_ONLY' |
    'NONE';
