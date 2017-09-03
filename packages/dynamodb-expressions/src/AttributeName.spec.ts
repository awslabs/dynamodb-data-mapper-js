import {
    isAttributeName,
    isListIndexAttributeName,
    isMapPropertyAttributeName,
    isScalarAttributeName,
    ListIndexAttributeName,
    MapPropertyAttributeName,
    ScalarAttributeName,
} from "./AttributeName";

const listAttributeName: ListIndexAttributeName = {
    listAttributeName: 'foo',
    index: 2,
};

const complexListAttributeName: ListIndexAttributeName = {
    listAttributeName: {
        mapAttributeName: {
            listAttributeName: {
                mapAttributeName: 'foo',
                propertyAttributeName: 'bar',
            },
            index: 3,
        },
        propertyAttributeName: 'baz',
    },
    index: 4,
};

const nestedAttributeName: MapPropertyAttributeName = {
    mapAttributeName: 'foo',
    propertyAttributeName: 'bar',
};

const complexNestedAttributeName: MapPropertyAttributeName = {
    mapAttributeName: {
        listAttributeName: {
            mapAttributeName: {
                listAttributeName: {
                    mapAttributeName: 'foo',
                    propertyAttributeName: 'bar',
                },
                index: 3,
            },
            propertyAttributeName: 'baz',
        },
        index: 4,
    },
    propertyAttributeName: {
        mapAttributeName: 'quux',
        propertyAttributeName: {
            mapAttributeName: 'snap',
            propertyAttributeName: {
                mapAttributeName: 'crackle',
                propertyAttributeName: {
                    listAttributeName: 'pop',
                    index: 2
                }
            }
        }
    },
};

const scalarAttributeName: ScalarAttributeName = 'foo';

const nonAttibuteNames = [
    null,
    void 0,
    true,
    false,
    0,
    21,
    Uint8Array.from([0xde, 0xad, 0xbe, 0xef]),
    [],
    {},
    {foo: 'bar'},
];

describe('isAttributeName', () => {
    it('should accept ListIndexAttributeName values', () => {
        expect(isAttributeName(listAttributeName)).toBe(true);
    });

    it('should accept complex ListIndexAttributeName values', () => {
        expect(isAttributeName(complexListAttributeName)).toBe(true);
    });

    it('should accept MapPropertyAttributeName values', () => {
        expect(isAttributeName(nestedAttributeName)).toBe(true);
    });

    it('should accept complex MapPropertyAttributeName values', () => {
        expect(isAttributeName(complexNestedAttributeName)).toBe(true);
    });

    it('should accept ScalarAttributeName values', () => {
        expect(isAttributeName(scalarAttributeName)).toBe(true);
    });

    it('should reject non-AttributeName values', () => {
        for (const nonName of nonAttibuteNames) {
            expect(isAttributeName(nonName)).toBe(false);
        }
    });
});

describe('isListIndexAttributeName', () => {
    it('should accept ListIndexAttributeName values', () => {
        expect(isListIndexAttributeName(listAttributeName)).toBe(true);
    });

    it('should accept complex ListIndexAttributeName values', () => {
        expect(isListIndexAttributeName(complexListAttributeName)).toBe(true);
    });

    it('should reject non-AttributeName values', () => {
        for (const nonName of nonAttibuteNames) {
            expect(isListIndexAttributeName(nonName)).toBe(false);
        }
    });
});

describe('isMapPropertyAttributeName', () => {
    it('should accept MapPropertyAttributeName values', () => {
        expect(isMapPropertyAttributeName(nestedAttributeName)).toBe(true);
    });

    it('should accept complex MapPropertyAttributeName values', () => {
        expect(isMapPropertyAttributeName(complexNestedAttributeName))
            .toBe(true);
    });

    it('should reject non-AttributeName values', () => {
        for (const nonName of nonAttibuteNames) {
            expect(isMapPropertyAttributeName(nonName)).toBe(false);
        }
    });
});

describe('isScalarAttributeName', () => {
    it('should accept ScalarAttributeName values', () => {
        expect(isScalarAttributeName(scalarAttributeName)).toBe(true);
    });

    it('should reject non-AttributeName values', () => {
        for (const nonName of nonAttibuteNames) {
            expect(isScalarAttributeName(nonName)).toBe(false);
        }
    });
});
