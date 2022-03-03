import {DbValueType} from '@ts-awesome/orm';

export interface ISqlQuery {
  sql: string;
  params?: {
    [key: string]: DbValueType;
  };
}

export interface IPgErrorResult {
  code: string;
  detail: string;
  name: string;
  error: string;
  table: string;
  constraint: string;
}
