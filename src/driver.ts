import {Pool} from 'pg';

import {ISqlDataDriver, ISqlTransaction} from '@viatsyshyn/ts-orm';
import {PgExecutor} from './executor';
import {PgTransaction} from './transaction';
import {injectable} from 'inversify';
import { ISqlQuery } from './interfaces';

@injectable()
export class PgDriver extends PgExecutor
  implements ISqlDataDriver<ISqlQuery> {
  constructor(
    private readonly pool: Pool
  ) {
    super(pool);
  }

  public async begin(): Promise<ISqlTransaction<ISqlQuery>> {
    const client = await this.pool.connect();
    await client.query('BEGIN');
    return new PgTransaction(client);
  }

  public end(): Promise<void> {
    return this.pool.end();
  }
}
