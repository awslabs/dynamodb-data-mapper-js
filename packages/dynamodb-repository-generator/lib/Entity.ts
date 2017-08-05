import {Schema} from "@aws/dynamodb-data-marshaller";

export class Entity {
    constructor(
        private readonly name: string,
        private readonly schema: Schema
    ) {}

    toString(): string {
        return `
${this.imports}

export interface ${this.name} {

}
`.trim();
    }

    private get imports(): string {
        return '';
    }

    private get members(): string {
        return '';
    }
}
