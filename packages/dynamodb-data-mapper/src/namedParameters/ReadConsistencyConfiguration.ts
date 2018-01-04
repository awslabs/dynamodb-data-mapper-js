import { ReadConsistency } from '../constants';

export interface ReadConsistencyConfiguration {
    /**
     * The read consistency to require when reading from DynamoDB.
     */
    readConsistency?: ReadConsistency;
}
