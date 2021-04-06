import { ReadConsistencyConfiguration } from './ReadConsistencyConfiguration';
import { StringToAnyObjectMap } from '../constants';
import { ProjectionExpression } from "@awslabs-community-fork/dynamodb-expressions";

export interface GetOptions extends ReadConsistencyConfiguration {
    /**
     * The item attributes to get.
     */
    projection?: ProjectionExpression;
}

/**
 * @deprecated
 */
export interface GetParameters<
    T extends StringToAnyObjectMap = StringToAnyObjectMap
> extends GetOptions {
    /**
     * The item being loaded.
     */
    item: T;
}
