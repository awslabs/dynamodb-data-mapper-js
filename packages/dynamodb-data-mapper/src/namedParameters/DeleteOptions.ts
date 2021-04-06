import { StringToAnyObjectMap } from '../constants';
import { ConditionExpression } from '@awslabs-community-fork/dynamodb-expressions';

export interface DeleteOptions {
    /**
     * A condition on which this delete operation's completion will be
     * predicated.
     */
    condition?: ConditionExpression;

    /**
     * The values to return from this operation.
     */
    returnValues?: 'ALL_OLD'|'NONE';

    /**
     * Whether this operation should NOT honor the version attribute specified
     * in the schema by incrementing the attribute and preventing the operation
     * from taking effect if the local version is out of date.
     */
    skipVersionCheck?: boolean;
}

/**
 * @deprecated
 */
export interface DeleteParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends DeleteOptions {
    /**
     * The item being deleted.
     */
    item: T;
}
