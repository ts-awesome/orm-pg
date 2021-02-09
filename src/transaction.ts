import {PoolClient} from 'pg';
import {PgExecutor} from './executor';
import {ISqlQuery} from './interfaces';
import {injectable} from 'inversify';
import {ITransaction} from '@ts-awesome/orm';

const COMMIT = 'COMMIT';
const ROLLBACK = 'ROLLBACK';

export const TRANSACTION_ALREADY_RESOLVED_ERROR = 'Tractions is already resolved.';

@injectable()
export class PgTransaction extends PgExecutor implements ITransaction<ISqlQuery> {
  private isFinished = false;

  constructor(
    private readonly conn: PoolClient
  ) {
    super(conn);
  }

  public get finished(): boolean {
    return this.isFinished;
  }

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
