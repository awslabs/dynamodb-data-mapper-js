import { ConditionExpression } from '@aws/dynamodb-expressions';

export interface ExecuteUpdateExpressionOptions {
    /**
     * A condition on which this update operation's completion will be
     * predicated.
     */
    condition?: ConditionExpression;
    returnValues?: 'ALL_NEW' | 'ALL_OLD' | 'UPDATED_NEW' | 'UPDATED_ALL' | 'NONE';
}
