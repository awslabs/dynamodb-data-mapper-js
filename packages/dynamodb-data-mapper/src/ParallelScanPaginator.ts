import { buildScanInput } from './buildScanInput';
import {
    ParallelScanOptions,
    ParallelScanState,
    ScanState,
} from './namedParameters';
import { Paginator } from './Paginator';
import { getSchema } from './protocols';
import {
    ParallelScanInput,
    ParallelScanPaginator as BasePaginator,
    ParallelScanState as BaseParallelScanState,
    ScanState as BaseScanState,
} from '@aws/dynamodb-query-iterator';
import {
    marshallKey,
    Schema,
    unmarshallItem,
    ZeroArgumentsConstructor,
} from '@aws/dynamodb-data-marshaller';
import { DynamoDB } from "@aws-sdk/client-dynamodb";

/**
 * Iterates over each page of items returned by a parallel DynamoDB scan until
 * no more pages are available.
 */
export class ParallelScanPaginator<T> extends Paginator<T> {
    private readonly _ctor: ZeroArgumentsConstructor<T>;
    private readonly _paginator: BasePaginator;
    private readonly _schema: Schema;

    constructor(
        client: DynamoDB,
        itemConstructor: ZeroArgumentsConstructor<T>,
        segments: number,
        options: ParallelScanOptions & { tableNamePrefix?: string } = {}
    ) {
        const schema = getSchema(itemConstructor.prototype);
        const input: ParallelScanInput = {
            ...buildScanInput(itemConstructor, options),
            TotalSegments: segments,
            ExclusiveStartKey: undefined,
            Segment: undefined
        };

        let scanState: BaseParallelScanState|undefined;
        if (options.scanState) {
            scanState = options.scanState.map(
                ({initialized, lastEvaluatedKey: lastKey}) => ({
                    initialized,
                    LastEvaluatedKey: lastKey
                        ? marshallKey(schema, lastKey, options.indexName)
                        : undefined
                } as BaseScanState)
            );
        }

        const paginator = new BasePaginator(client, input, scanState);
        super(paginator, itemConstructor);

        this._paginator = paginator;
        this._ctor = itemConstructor;
        this._schema = schema;
    }

    /**
     * The `lastEvaluatedKey` attribute is not available on parallel scans. Use
     * {@link scanState} instead.
     */
    get lastEvaluatedKey() {
        return undefined;
    }

    /**
     * A snapshot of the current state of a parallel scan. May be used to resume
     * a parallel scan with a separate paginator.
     */
    get scanState(): ParallelScanState {
        return this._paginator.scanState.map(
            ({initialized, LastEvaluatedKey}) => ({
                initialized,
                lastEvaluatedKey: LastEvaluatedKey
                    ? unmarshallItem(this._schema, LastEvaluatedKey, this._ctor)
                    : undefined
            } as ScanState)
        );
    }
}
