export const VERSION = '0.2.1';

export const MAX_WRITE_BATCH_SIZE = 25;

export type OnMissingStrategy = 'remove'|'skip';


export type ReadConsistency = 'eventual'|'strong';

export type SyncOrAsyncIterable<T> = Iterable<T>|AsyncIterable<T>;
