import { StringToAnyObjectMap } from '../constants';
import { ConditionExpression } from '@awslabs-community-fork/dynamodb-expressions';

export interface PutOptions {
    /**
     * A condition on whose evaluation this put operation's completion will be
     * predicated.
     */
    condition?: ConditionExpression;

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
export interface PutParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends PutOptions {
    /**
     * The object to be saved.
     */
    item: T;
}
