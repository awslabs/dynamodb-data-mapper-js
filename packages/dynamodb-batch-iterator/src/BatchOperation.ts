import {
    BatchState,
    SyncOrAsyncIterable,
    TableState,
    TableStateElement,
    ThrottledTableConfiguration,
} from './types';
const DynamoDB = require('aws-sdk/clients/dynamodb');

if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("__@@asyncIterator__");
}

export abstract class BatchOperation<
    Element extends TableStateElement
> implements AsyncIterableIterator<[string, Element]> {
    /**
     * The maximum number of elements that may be included in a single batch.
     */
    protected abstract readonly batchSize: number;

    /**
     * Items that have been retrieved and are ready to be returned.
     */
    protected readonly pending: Array<[string, Element]> = [];

    /**
     * A mapping of table names to table-specific operation state (e.g., the
     * number of throttling events experienced, etc.)
     */
    protected readonly state: BatchState<Element> = {};

    /**
     * Input elements that are prepared for immediate dispatch
     */
    protected readonly toSend: Array<[string, Element]> = [];

    private readonly throttled = new Set<Promise<ThrottledTableConfiguration<Element>>>();
    private readonly iterator: Iterator<[string, Element]>|AsyncIterator<[string, Element]>;
    private sourceDone: boolean = false;
    private sourceNext: IteratorResult<[string, Element]>|Promise<IteratorResult<[string, Element]>>;
    private lastResolved?: Promise<IteratorResult<[string, Element]>>;

    /**
     * @param client    The AWS SDK client with which to communicate with
     *                  DynamoDB.
     * @param items     A synchronous or asynchronous iterable of tuples
     *                  describing the operations to execute. The first member
     *                  of the tuple should be the name of the table targeted by
     *                  the operation.
     */
    constructor(
        protected readonly client: DynamoDB,
        items: SyncOrAsyncIterable<[string, Element]>
    ) {
        if (isIterable(items)) {
            this.iterator = items[Symbol.iterator]();
        } else {
            this.iterator = items[Symbol.asyncIterator]();
        }
        this.sourceNext = this.iterator.next();
    }

    next(): Promise<IteratorResult<[string, Element]>> {
        if (this.lastResolved) {
            this.lastResolved = this.lastResolved.then(() => this.getNext());
        } else {
            this.lastResolved = this.getNext();
        }

        return this.lastResolved;
    }

    [Symbol.asyncIterator]() {
        return this;
    }

    /**
     * Execute a single batch request and process the result.
     */
    protected abstract doBatchRequest(): Promise<void>;

    /**
     * Create and return the initial state object for a given DynamoDB table.
     *
     * @param tableName The name of the table whose initial state should be
     *                  returned.
     */
    protected getInitialTableState(tableName: string): TableState<Element> {
        return {
            backoffFactor: 0,
            name: tableName,
        };
    }

    /**
     * Accept an array of unprocessed items belonging to a single table and
     * re-enqueue it for submission, making sure the appropriate level of
     * backoff is applied to future operations on the same table.
     *
     * @param tableName     The table to which the unprocessed elements belong.
     * @param unprocessed   Elements returned by DynamoDB as not yet processed.
     *                      The elements should not be unmarshalled, but they
     *                      should be reverted to the form used for elements
     *                      that have not yet been sent.
     */
    protected handleThrottled(
        tableName: string,
        unprocessed: Array<Element>
    ): void {
        const tableState = this.state[tableName];
        tableState.backoffFactor++;

        if (tableState.tableThrottling) {
            this.throttled.delete(tableState.tableThrottling.backoffWaiter);
            unprocessed.unshift(...tableState.tableThrottling.unprocessed);
        }

        tableState.tableThrottling = {
            unprocessed,
            backoffWaiter: new Promise(resolve => {
                setTimeout(
                    resolve,
                    exponentialBackoff(tableState.backoffFactor),
                    tableState
                );
            })
        };

        this.throttled.add(tableState.tableThrottling.backoffWaiter);
    }

    /**
     * Iterate over all pending writes and move those targeting throttled tables
     * into the throttled queue.
     *
     * @param unprocessedTables     A set of tables for which some items were
     *                              returned without being processed.
     */
    protected movePendingToThrottled(unprocessedTables: Set<string>) {
        for (let i = this.toSend.length - 1; i > -1; i--) {
            const [table, attributes] = this.toSend[i];
            if (unprocessedTables.has(table)) {
                (this.state[table] as ThrottledTableConfiguration<Element>)
                    .tableThrottling.unprocessed.push(attributes);
                this.toSend.splice(i, 1);
            }
        }
    }

    private addToSendQueue([tableName, attributes]: [string, Element]): void {
        if (!this.state[tableName]) {
            this.state[tableName] = this.getInitialTableState(tableName);
        }
        const tableState = this.state[tableName];

        if (tableState.tableThrottling) {
            tableState.tableThrottling.unprocessed.push(attributes);
        } else {
            this.toSend.push([tableName, attributes]);
        }
    }

    private enqueueThrottled(
        table: ThrottledTableConfiguration<Element>
    ): void {
        const {
            tableThrottling: {backoffWaiter, unprocessed}
        } = table;
        if (unprocessed.length > 0) {
            this.toSend.push(...unprocessed.map(
                attr => [table.name, attr] as [string, Element]
            ));
        }

        this.throttled.delete(backoffWaiter);
        delete table.tableThrottling;
    }

    private async getNext(): Promise<IteratorResult<[string, Element]>> {
        if (
            this.sourceDone &&
            this.pending.length === 0 &&
            this.toSend.length === 0 &&
            this.throttled.size === 0
        ) {
            return {done: true} as IteratorResult<[string, Element]>;
        }

        if (this.pending.length > 0) {
            return {
                done: false,
                value: this.pending.shift() as [string, Element]
            };
        }

        await this.refillPending();
        return this.getNext();
    }

    private async refillPending() {
        while (
            !this.sourceDone &&
            this.toSend.length < this.batchSize
        ) {
            const toProcess = isIteratorResult(this.sourceNext)
                ? this.sourceNext
                : await Promise.race([
                    this.sourceNext,
                    Promise.race(this.throttled)
                ]);

            if (isIteratorResult(toProcess)) {
                this.sourceDone = toProcess.done;
                if (!this.sourceDone) {
                    this.addToSendQueue(toProcess.value);
                    this.sourceNext = this.iterator.next();
                }
            } else {
                this.enqueueThrottled(toProcess);
            }
        }

        while (this.toSend.length < this.batchSize && this.throttled.size > 0) {
            this.enqueueThrottled(await Promise.race(this.throttled));
        }

        if (this.toSend.length > 0) {
            await this.doBatchRequest();
        }
    }
}

function exponentialBackoff(attempts: number) {
    return Math.floor(Math.random() * Math.pow(2, attempts));
}

function isIterable<T>(arg: any): arg is Iterable<T> {
    return Boolean(arg) && typeof arg[Symbol.iterator] === 'function';
}

function isIteratorResult<T>(arg: any): arg is IteratorResult<T> {
    return Boolean(arg) && typeof arg.done === 'boolean';
}
