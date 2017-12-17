import { BatchWrite } from './BatchWrite';

describe('BatchWrite', () => {
    it('should return itself when its Symbol.asyncIterator method is called', () => {
        const batchWrite = new BatchWrite({} as any, []);
        expect(batchWrite[Symbol.asyncIterator]()).toBe(batchWrite);
    });
});
