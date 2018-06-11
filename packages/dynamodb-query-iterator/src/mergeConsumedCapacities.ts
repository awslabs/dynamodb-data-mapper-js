import {
    Capacity,
    ConsumedCapacity,
    SecondaryIndexesCapacityMap,
} from 'aws-sdk/clients/dynamodb';

/**
 * @internal
 */
export function mergeConsumedCapacities(
    a?: ConsumedCapacity,
    b?: ConsumedCapacity
): ConsumedCapacity|undefined {
    if (a || b) {
        a = a || {};
        b = b || {};

        if ((a.TableName && b.TableName) && a.TableName !== b.TableName) {
            throw new Error(
                'Consumed capacity reports may only be merged if they describe the same table'
            );
        }

        return {
            TableName: a.TableName || b.TableName,
            CapacityUnits: (a.CapacityUnits || 0) + (b.CapacityUnits || 0),
            Table: mergeCapacities(a.Table, b.Table),
            LocalSecondaryIndexes: mergeCapacityMaps(
                a.LocalSecondaryIndexes,
                b.LocalSecondaryIndexes
            ),
            GlobalSecondaryIndexes: mergeCapacityMaps(
                a.GlobalSecondaryIndexes,
                b.GlobalSecondaryIndexes
            ),
        }
    }
}

function mergeCapacities(a?: Capacity, b?: Capacity): Capacity|undefined {
    if (a || b) {
        return {
            CapacityUnits: ((a && a.CapacityUnits) || 0) +
                ((b && b.CapacityUnits) || 0),
        };
    }
}

function mergeCapacityMaps(
    a?: SecondaryIndexesCapacityMap,
    b?: SecondaryIndexesCapacityMap
): SecondaryIndexesCapacityMap|undefined {
    if (a || b) {
        const out: SecondaryIndexesCapacityMap = {};

        a = a || {};
        b = b || {};
        const keys = new Set<string>();
        for (const map of [a, b]) {
            for (const indexName of Object.keys(map)) {
                keys.add(indexName);
            }
        }

        for (const key of keys) {
            out[key] = mergeCapacities(a[key], b[key])!;
        }

        return out;
    }
}
