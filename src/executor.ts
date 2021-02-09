import {Pool, PoolClient, types} from "pg";
import * as yesql from "yesql";
import {IQueryData, IQueryExecutor} from "@ts-awesome/orm";
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

  public async execute<TResult>(sqlQuery: ISqlQuery): Promise<IQueryData[]> {
    if (!sqlQuery || !sqlQuery.sql || sqlQuery.sql.trim() === "") {
      return Promise.reject(new Error("sqlQuery is not provided"));
    }

    if (!sqlQuery.params) {
      sqlQuery.params = {};
    }

    let fixedSql = yesql.pg(sqlQuery.sql)(sqlQuery.params);
    try {
      let res = await this.queryExecutor.query(fixedSql);
      return res.rows;
    } catch (err) {
      switch (err.code) {
        case DUPLICATE_VALUE_DB_ERROR_CODE:
          throw new DuplicateValueDbError(err);
        case FK_VIOLATES_DB_ERROR_CODE:
          throw new FkViolatedDbError(err.detail, err.error);
        default:
          throw new DbError(err.code, undefined, err.detail, err.error);
      }
    }
  }
}
