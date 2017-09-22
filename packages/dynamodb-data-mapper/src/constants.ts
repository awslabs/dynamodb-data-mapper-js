export enum ReadConsistency {
    EventuallyConsistent = 'EventuallyConsistent',
    StronglyConsistent = 'StronglyConsistent',
}

export enum OnMissingStrategy {
    Remove = 'Remove',
    Skip = 'Skip',
}
