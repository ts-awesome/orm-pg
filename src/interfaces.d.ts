import {DbValueType, IQueryExecutor} from "@viatsyshyn/ts-orm";

export interface ISqlTransaction extends IQueryExecutor<ISqlQuery> {
  readonly finished: boolean;
  commit(): Promise<void>;
  rollback(): Promise<void>;
}

export interface ISqlQuery {
  sql: string
  params?: {[key: string]: DbValueType}
}

export interface IPgErrorResult {
  code: string;
  detail: string;
  name: string;
  error: string;
  table: string;
  constraint: string;
}

export interface ISqlDataDriver extends IQueryExecutor<ISqlQuery> {
  begin(): Promise<ISqlTransaction>;
  end(): Promise<void>;
}
