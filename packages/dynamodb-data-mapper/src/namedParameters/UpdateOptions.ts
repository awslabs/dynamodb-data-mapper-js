import { OnMissingStrategy, StringToAnyObjectMap } from '../constants';
import { ConditionExpression } from '@awslabs-community-fork/dynamodb-expressions';

export interface UpdateOptions {
    /**
     * A condition on whose evaluation this update operation's completion will
     * be predicated.
     */
    condition?: ConditionExpression;

    /**
     * Whether the absence of a value defined in the schema should be treated as
     * a directive to remove the property from the item.
     */
    onMissing?: OnMissingStrategy;

    /**
     * Whether this operation should NOT honor the version attribute specified
     * in the schema by incrementing the attribute and preventing the operation
     * from taking effect if the local version is out of date.
     */
    skipVersionCheck?: boolean;
}

export interface UpdateParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends UpdateOptions {
    /**
     * The object to be saved.
     */
    item: T;
}
