import {PoolClient} from 'pg';
import {PgExecutor} from './executor';
import {ISqlQuery} from './interfaces';
import {injectable, unmanaged} from 'inversify';
import {IQueryData, IsolationLevel, ITransaction} from '@ts-awesome/orm';
import {BaseTransaction} from "@ts-awesome/orm/dist/base";
import {WithParams} from "@ts-awesome/orm/dist/interfaces";

const COMMIT = 'COMMIT';
const ROLLBACK = 'ROLLBACK';
const SET_TRANSACTION_ISOLATION_LEVEL = 'SET TRANSACTION ISOLATION LEVEL';

export const TRANSACTION_ALREADY_RESOLVED_ERROR = 'Transaction is already resolved.';
export const UNSUPPORTED_ISOLATION_LEVEL = 'Unsupported isolation level ';

export type PgTransactionalExecutorClient = Pick<PoolClient, 'query' | 'release'>;

@injectable()
export class PgTransaction extends BaseTransaction<ISqlQuery> implements ITransaction<ISqlQuery> {
  private isFinished = false;
  private executor: PgExecutor

  constructor(
    @unmanaged() private readonly conn: PgTransactionalExecutorClient
  ) {
    super();
    this.executor = new PgExecutor(conn)
  }

  protected do(query: ISqlQuery & WithParams): Promise<readonly IQueryData[]> {
    return this.executor.execute(query);
  }

  public get finished(): boolean {
    return this.isFinished;
  }

  // noinspection JSUnusedGlobalSymbols
  public async setIsolationLevel(isolationLevel: IsolationLevel): Promise<void> {
    if (this.finished) {
      throw new Error(TRANSACTION_ALREADY_RESOLVED_ERROR);
    }

    if (!isSupportedIsolationLevel(isolationLevel as string)) {
      throw new Error(UNSUPPORTED_ISOLATION_LEVEL + JSON.stringify(isolationLevel))
    }

    await this.conn.query(`${SET_TRANSACTION_ISOLATION_LEVEL} ${isolationLevel}`);
  }

  // noinspection JSUnusedGlobalSymbols
  public async commit(): Promise<void> {
    if (this.finished) {
      throw new Error(TRANSACTION_ALREADY_RESOLVED_ERROR);
    }

    try {
      await this.conn.query(COMMIT);
    } finally {
      this.conn.release();
      this.isFinished = true;
    }
  }

  // noinspection JSUnusedGlobalSymbols
  public async rollback(): Promise<void> {
    if (this.finished) {
      throw new Error(TRANSACTION_ALREADY_RESOLVED_ERROR);
    }

    try {
      await this.conn.query(ROLLBACK);
    } finally {
      this.conn.release();
      this.isFinished = true;
    }
  }
}

export function isSupportedIsolationLevel(isolationLevel: string): boolean {
  switch (isolationLevel) {
    case 'SERIALIZABLE':
    case 'REPEATABLE READ':
    case 'READ COMMITTED':
    case 'READ UNCOMMITTED':
      return true;
  }
  return false;
}
