import {Pool} from 'pg';

import {IQueryDriver, ITransaction} from '@viatsyshyn/ts-orm';
import {PgExecutor} from './executor';
import {PgTransaction} from './transaction';
import {injectable, unmanaged} from 'inversify';
import { ISqlQuery } from './interfaces';

const BEGIN = 'BEGIN';

@injectable()
export class PgDriver extends PgExecutor
  implements IQueryDriver<ISqlQuery> {
  constructor(
    @unmanaged() private readonly pool: Pool
  ) {
    super(pool);
  }

  public async begin(): Promise<ITransaction<ISqlQuery>> {
    const client = await this.pool.connect();
    await client.query(BEGIN);
    return new PgTransaction(client);
  }

  public end(): Promise<void> {
    return this.pool.end();
  }
}
