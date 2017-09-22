import {DataMapper, StringToAnyObjectMap} from "./DataMapper";
import {TableDefinition} from "@aws/dynamodb-data-marshaller";
import {
    DataMapperConfiguration,
    DataMapperParameters,
    DeleteParameters,
    GetParameters,
    PutParameters,
    QueryParameters,
    ScanParameters,
    UpdateParameters,
} from "./namedParameters";

export class Repository<T extends StringToAnyObjectMap> {
    private readonly dataMapper: DataMapper;
    private readonly tableDefinition: TableDefinition;

    constructor({
        tableDefinition,
        ...rest,
    }: DataMapperConfiguration & DataMapperParameters) {
        this.dataMapper = new DataMapper(rest);
        this.tableDefinition = tableDefinition;
    }

    delete(parameters: DeleteParameters<T>): Promise<T|undefined> {
        return this.dataMapper.delete<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    get(parameters: GetParameters): Promise<T> {
        return this.dataMapper.get<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    put(parameters: PutParameters<T>): Promise<T|undefined> {
        return this.dataMapper.put({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    query(parameters: QueryParameters): AsyncIterableIterator<T> {
        return this.dataMapper.query<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    scan(parameters: ScanParameters): AsyncIterableIterator<T> {
        return this.dataMapper.scan<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    update(parameters: UpdateParameters<T>): Promise<T> {
        return this.dataMapper.update({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }
}
