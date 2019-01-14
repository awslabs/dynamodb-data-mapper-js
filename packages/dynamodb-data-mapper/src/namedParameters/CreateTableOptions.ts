import { ProvisionedThroughput } from './ProvisionedThroughput';
import { PerIndexOptions } from './SecondaryIndexOptions';

export interface CreateTableOptions extends ProvisionedThroughput {
    streamViewType?: StreamViewType;
    indexOptions?: PerIndexOptions;
    sseSpecification?: SseSpecification;
}

export interface SseSpecification {
    enabled: boolean;
    sseType?: SseType;
    kmsMasterKeyId?: string;
}

/**
 * Server-side encryption type:
 *   AES256 - Server-side encryption which uses the AES256 algorithm (not applicable).
 *   KMS - Server-side encryption which uses AWS Key Management Service.
 */
export type SseType = 'AES256' | 'KMS'

export type StreamViewType =
    'NEW_IMAGE' |
    'OLD_IMAGE' |
    'NEW_AND_OLD_IMAGES' |
    'KEYS_ONLY' |
    'NONE';
