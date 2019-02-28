import {IPgErrorResult} from "./interfaces";

export const DUPLICATE_VALUE_DB_ERROR_CODE = '23505';
export const FK_VIOLATES_DB_ERROR_CODE = '23503';

export class DbError extends Error {
  public code: string;

  constructor(code: string, name?: string, message?: string, stack?: string) {
    super();

    this.code = code;
    this.name = name || "DB_ERROR";
    if (message) {
      this.message = message;
    }
    if (stack) {
      this.stack = stack;
    }

    Object.setPrototypeOf(this, DbError.prototype);
  }
}

export class DuplicateValueDbError extends DbError {

  public table: string;
  public column: string;
  public value: string;

  constructor(pgErrorResult: IPgErrorResult) {
    super(DUPLICATE_VALUE_DB_ERROR_CODE, 'DUPLICATE_VALUE_ERROR', pgErrorResult.detail, pgErrorResult.error);

    this.table = pgErrorResult.table;

    let details = pgErrorResult.detail;
    if (details) {
      this.column = details.slice(details.indexOf('(') + 1, details.indexOf(')'));
      this.value = details.slice(details.lastIndexOf('(') + 1, details.lastIndexOf(')'));
    }
    Object.setPrototypeOf(this, DuplicateValueDbError.prototype);
  }
}

export class FkViolatedDbError extends DbError {
  constructor(message?: string, stack?: string) {
    super(FK_VIOLATES_DB_ERROR_CODE, 'FK_VIOLATED_ERROR', message, stack);

    Object.setPrototypeOf(this, FkViolatedDbError.prototype);
  }
}
