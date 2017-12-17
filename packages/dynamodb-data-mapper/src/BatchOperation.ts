import {
    BatchState,
    PreparedElement,
    TableStateElement,
    ThrottledTableConfiguration,
    WriteType,
} from './BatchTypes';
import {
    StringToAnyObjectMap,
    SyncOrAsyncIterable,
} from './constants';
import { getTableName } from './protocols';
import DynamoDB = require('aws-sdk/clients/dynamodb');

export abstract class BatchOperation<
    T extends StringToAnyObjectMap,
    Element extends T|[WriteType, T],
    E extends TableStateElement
> implements AsyncIterableIterator<T> {
    /**
     * The maximum number of elements that may be included in a single batch.
     */
    protected abstract readonly batchSize: number;

    /**
     * Items that have been retrieved and unmarshalled.
     */
    protected readonly pending: Array<T> = [];

    /**
     * A mapping of table names to table-specific operation state (e.g., the
     * number of throttling events experienced, item schemata and constructors,
     * etc.)
     */
    protected readonly state: BatchState<T, E> = {};

    /**
     * Input elements that are prepared for immediate dispatch
     */
    protected readonly toSend: Array<[string, E]> = [];
    private readonly throttled = new Set<Promise<ThrottledTableConfiguration<T, E>>>();
    private readonly iterator: Iterator<Element>|AsyncIterator<Element>;
    private sourceDone: boolean = false;
    private sourceNext: IteratorResult<Element>|Promise<IteratorResult<Element>>;

    constructor(
        protected readonly client: DynamoDB,
        items: SyncOrAsyncIterable<Element>,
        private readonly tableNamePrefix: string = ''
    ) {
        if (isIterable(items)) {
            this.iterator = items[Symbol.iterator]();
        } else {
            this.iterator = items[Symbol.asyncIterator]();
        }
        this.sourceNext = this.iterator.next();
    }

    async next(): Promise<IteratorResult<T>> {
        if (
            this.sourceDone &&
            this.pending.length === 0 &&
            this.toSend.length === 0 &&
            this.throttled.size === 0
        ) {
            return {done: true} as IteratorResult<T>;
        }

        if (this.pending.length > 0) {
            return {
                done: false,
                value: this.pending.shift() as T
            };
        }

        await this.refillPending();
        return this.next();
    }

    [Symbol.asyncIterator]() {
        return this;
    }

    protected abstract doBatchRequest(): Promise<void>;

    protected abstract prepareElement(item: Element): PreparedElement<T, E>;

    protected handleThrottled(
        tableName: string,
        unprocessed: Array<E>
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
            const [table, marshalled] = this.toSend[i];
            if (unprocessedTables.has(table)) {
                (this.state[table] as ThrottledTableConfiguration<T, E>)
                    .tableThrottling.unprocessed.push(marshalled);
                this.toSend.splice(i, 1);
            }
        }
    }

    protected getTableName(item: StringToAnyObjectMap): string {
        return getTableName(item, this.tableNamePrefix);
    }

    private addToSendQueue(item: Element): void {
        const {tableName, tableState, marshalled} = this.prepareElement(item);

        if (tableState.tableThrottling) {
            tableState.tableThrottling.unprocessed.push(marshalled);
        } else {
            this.toSend.push([tableName, marshalled]);
        }
    }

    private enqueueThrottled(table: ThrottledTableConfiguration<T, E>): void {
        const {
            tableThrottling: {backoffWaiter, unprocessed}
        } = table;
        if (unprocessed.length > 0) {
            this.toSend.push(...unprocessed.map(
                attr => [table.name, attr] as [string, E]
            ));
        }

        this.throttled.delete(backoffWaiter);
        delete table.tableThrottling;
    }

    private async refillPending() {
        while (
            !this.sourceDone &&
            this.toSend.length < this.batchSize
        ) {
            const toProcess = await Promise.race([
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

        if (this.toSend.length) {
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
