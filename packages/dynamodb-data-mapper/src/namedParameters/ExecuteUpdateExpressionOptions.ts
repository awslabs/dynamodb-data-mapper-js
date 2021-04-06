import { ConditionExpression } from '@awslabs-community-fork/dynamodb-expressions';

export interface ExecuteUpdateExpressionOptions {
    /**
     * A condition on which this update operation's completion will be
     * predicated.
     */
    condition?: ConditionExpression;
}
