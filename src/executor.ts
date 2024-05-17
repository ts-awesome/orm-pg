import {Pool, QueryConfig, types} from "pg";
import {pg} from "yesql";
import {IQueryData, WithParams} from "@ts-awesome/orm";
import {BaseExecutor} from "@ts-awesome/orm/dist/base";

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

export type PgExecutorClient = Pick<Pool, 'query'>;

export class PgExecutor extends BaseExecutor<ISqlQuery, IQueryData> {
  constructor(
    private readonly queryExecutor: PgExecutorClient
  ) {
    super()
  }

  protected async do(input: ISqlQuery & WithParams): Promise<readonly IQueryData[]> {
    const query = this.prepareQuery(input);
    try {
      const {rows, rowCount} = await this.queryExecutor.query(query);
      return rowCount > 0 ? rows : [];
    } catch (err) {
      if (err.code == null || err.detail == null) {
        // keep as is
        throw err;
      }

      switch (err.code) {
        case DUPLICATE_VALUE_DB_ERROR_CODE:
          throw new DuplicateValueDbError(err);
        case FK_VIOLATES_DB_ERROR_CODE:
          throw new FkViolatedDbError(err.detail, err.error);
      }

      throw new DbError(err.code, undefined, err.detail ?? err.message, err.error?.stack ?? err.error ?? err?.stack);
    }
  }

  private prepareQuery(query?: ISqlQuery): QueryConfig {
    return injectQueryParams(query);
  }
}

export function injectQueryParams(query?: ISqlQuery): QueryConfig {
  const sql = query?.sql?.trim() ?? '';
  if (sql === '') {
    throw new Error(`Invalid SQL query ${JSON.stringify(query)}`);
  }

  return pg(sql)(query?.params ?? {});
}
