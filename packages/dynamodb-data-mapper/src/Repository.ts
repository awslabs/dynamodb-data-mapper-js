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

/**
 * A wrapper around a {DataMapper} that can perform any of its underlying
 * operations with a static {TableDefinition} provided to the constructor.
 */
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

    /**
     * Perform a DeleteItem operation using the bound {TableDefinition}.
     */
    delete(parameters: DeleteParameters<T>): Promise<T|undefined> {
        return this.dataMapper.delete<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    /**
     * Perform a GetItem operation using the bound {TableDefinition}.
     */
    get(parameters: GetParameters): Promise<T> {
        return this.dataMapper.get<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    /**
     * Perform a PutItem operation using the bound {TableDefinition}.
     */
    put(parameters: PutParameters<T>): Promise<T|undefined> {
        return this.dataMapper.put({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    /**
     * Perform a Query operation using the bound {TableDefinition}.
     *
     * @return An asynchronous iterator that yields query results. Intended
     * to be consumed with a `for await ... of` loop.
     */
    query(parameters: QueryParameters): AsyncIterableIterator<T> {
        return this.dataMapper.query<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    /**
     * Perform a Scan operation using the bound {TableDefinition}.
     *
     * @return An asynchronous iterator that yields scan results. Intended
     * to be consumed with a `for await ... of` loop.
     */
    scan(parameters: ScanParameters): AsyncIterableIterator<T> {
        return this.dataMapper.scan<T>({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }

    /**
     * Perform an UpdateItem operation using the bound {TableDefinition}.
     */
    update(parameters: UpdateParameters<T>): Promise<T> {
        return this.dataMapper.update({
            ...parameters,
            tableDefinition: this.tableDefinition,
        });
    }
}
