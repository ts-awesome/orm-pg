import {Pool} from 'pg';

import {IQueryDriver, IsolationLevel, ITransaction} from '@ts-awesome/orm';
import {PgExecutor} from './executor';
import {isSupportedIsolationLevel, PgTransaction, UNSUPPORTED_ISOLATION_LEVEL} from './transaction';
import {injectable, unmanaged} from 'inversify';
import { ISqlQuery } from './interfaces';

const BEGIN = 'BEGIN TRANSACTION';

type PgTransactionalExecutor = Pick<Pool, 'query' | 'connect' | 'end'>;

@injectable()
export class PgDriver extends PgExecutor
  implements IQueryDriver<ISqlQuery> {
  constructor(
    @unmanaged() private readonly pool: PgTransactionalExecutor
  ) {
    super(pool);
  }

  public async begin(isolationLevel?: IsolationLevel): Promise<ITransaction<ISqlQuery>> {
    const client = await this.pool.connect();

    if (isolationLevel && !isSupportedIsolationLevel(isolationLevel as string)) {
      throw new Error(UNSUPPORTED_ISOLATION_LEVEL + JSON.stringify(isolationLevel))
    }

    await client.query(BEGIN + (isSupportedIsolationLevel(isolationLevel) ? ' ISOLATION LEVEL ' + isolationLevel : ''));
    return new PgTransaction(client);
  }

  public end(): Promise<void> {
    return this.pool.end();
  }
}
