import {Pool, PoolClient, types} from "pg";
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

// Add TIME_STAMPT parser
const TIME_STAMPT_CODE = 1114;
(<any>types).setTypeParser(TIME_STAMPT_CODE, (val: string) => {
  return new Date(val.replace(' ', 'T') + 'Z');
});

@injectable()
export class PgExecutor implements IQueryExecutor<ISqlQuery> {

  constructor(private readonly queryExecutor: Pool | PoolClient) {}

  public async execute(query: ISqlQuery): Promise<ReadonlyArray<IQueryData>>;
  public async execute(query: ISqlQuery, count: true): Promise<number>;
  public async execute<X extends TableMetaProvider<any>>(query: ISqlQuery, Model: X, sensitive?: boolean): Promise<ReadonlyArray<InstanceType<X>>>;
  public async execute<TResult>(sqlQuery: ISqlQuery, Model?: any, sensitive = false): Promise<any> {
    if (!sqlQuery || !sqlQuery.sql || sqlQuery.sql.trim() === "") {
      throw new Error("sqlQuery is not provided");
    }

    let fixedSql = yesql.pg(sqlQuery.sql)(sqlQuery.params ?? {});
    try {
      const {rows} = await this.queryExecutor.query(fixedSql);
      return reader(rows, Model, sensitive);
    } catch (err) {
      switch (err.code) {
        case DUPLICATE_VALUE_DB_ERROR_CODE: throw new DuplicateValueDbError(err);
        case FK_VIOLATES_DB_ERROR_CODE:     throw new FkViolatedDbError(err.detail, err.error);
        default:                            throw new DbError(err.code, undefined, err.detail, err.error);
      }
    }
  }
}
