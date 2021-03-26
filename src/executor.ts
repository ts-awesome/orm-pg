import {Pool, types} from "pg";
import * as yesql from "yesql";
import {IQueryData, IQueryExecutor, reader, TableMetaProvider} from "@ts-awesome/orm";
import {injectable} from "inversify";
import {ISqlQuery} from "./interfaces";
import {
  DbError,
  DUPLICATE_VALUE_DB_ERROR_CODE,
  DuplicateValueDbError,
  FK_VIOLATES_DB_ERROR_CODE,
  FkViolatedDbError
} from "./errors";

{
  // Add TIME_STAMP parser
  const TIME_STAMP_CODE = 1114;
  (<any>types).setTypeParser(
    TIME_STAMP_CODE,
    (val: string) => new Date(val.replace(' ', 'T') + 'Z')
  );
}

type PgExecutorClient = Pick<Pool, 'query'>;

@injectable()
export class PgExecutor implements IQueryExecutor<ISqlQuery> {

  constructor(private readonly queryExecutor: PgExecutorClient) {}

  public async execute(query: ISqlQuery): Promise<ReadonlyArray<IQueryData>>;
  public async execute(query: ISqlQuery, scalar: true): Promise<number>;
  public async execute<X extends TableMetaProvider>(query: ISqlQuery, Model: X, sensitive?: boolean): Promise<ReadonlyArray<InstanceType<X>>>;
  public async execute(sqlQuery: ISqlQuery, Model?: unknown | true, sensitive = false): Promise<any> {
    if (!sqlQuery || !sqlQuery.sql || sqlQuery.sql.trim() === "") {
      throw new Error("sqlQuery is not provided");
    }

    const fixedSql = yesql.pg(sqlQuery.sql)(sqlQuery.params ?? {});
    let rows = [];
    try {
      ({rows} = await this.queryExecutor.query(fixedSql));
    } catch (err) {
      if (err.code == null) {
        // comes from reader
        throw err;
      }
      console.error('DB_ERROR', err);
      switch (err.code) {
        case DUPLICATE_VALUE_DB_ERROR_CODE: throw new DuplicateValueDbError(err);
        case FK_VIOLATES_DB_ERROR_CODE:     throw new FkViolatedDbError(err.detail, err.error);
        default:                            throw new DbError(err.code, undefined, err.detail, err.error);
      }
    }
    return reader(rows, Model as any, sensitive);
  }
}
