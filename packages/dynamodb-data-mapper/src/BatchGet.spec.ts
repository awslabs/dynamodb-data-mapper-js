import { BatchGet } from './BatchGet';

describe('BatchGet', () => {
    it('should return itself when its Symbol.asyncIterator method is called', () => {
        const batchGet = new BatchGet({} as any, []);
        expect(batchGet[Symbol.asyncIterator]()).toBe(batchGet);
    });
});
