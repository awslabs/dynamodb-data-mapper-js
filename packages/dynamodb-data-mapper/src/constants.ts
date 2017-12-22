export const VERSION = '0.2.1';

export const MAX_WRITE_BATCH_SIZE = 25;

export const MAX_READ_BATCH_SIZE = 100;

export type OnMissingStrategy = 'remove'|'skip';

export type ReadConsistency = 'eventual'|'strong';


export interface StringToAnyObjectMap {[key: string]: any;}

export type SyncOrAsyncIterable<T> = Iterable<T>|AsyncIterable<T>;

export type WriteType = 'put'|'delete';
