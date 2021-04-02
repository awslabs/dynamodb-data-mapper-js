import { ReadConsistency } from '../constants';
import {DynamoDB} from "@aws-sdk/client-dynamodb";

export interface DataMapperConfiguration {
    /**
     * The low-level DynamoDB client to use to execute API operations.
     */
    client: DynamoDB;

    /**
     * The default read consistency to use when loading items. If not specified,
     * 'eventual' will be used.
     */
    readConsistency?: ReadConsistency;

    /**
     * Whether operations should NOT by default honor the version attribute
     * specified in the schema by incrementing the attribute and preventing the
     * operation from taking effect if the local version is out of date.
     */
    skipVersionCheck?: boolean;

    /**
     * A prefix to apply to all table names.
     */
    tableNamePrefix?: string;
}
