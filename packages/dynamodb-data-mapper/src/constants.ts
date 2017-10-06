export const ReadConsistencies = {
    eventual: true,
    strong: true,
};

export type ReadConsistency = keyof typeof ReadConsistencies;

export const OnMissingStrategies = {
    remove: true,
    skip: true,
};

export type OnMissingStrategy = keyof typeof OnMissingStrategies;
