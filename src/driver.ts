import {Pool} from 'pg';

import {ISqlDataDriver, ISqlTransaction} from "./interfaces";
import {PgExecutor} from "./executor";
import {PgTransaction} from "./transaction";
import {injectable} from "inversify";

@injectable()
export class PgDriver extends PgExecutor
  implements ISqlDataDriver {
  constructor(
    private readonly pool: Pool
  ) {
    super(pool);
  }

  public async begin(): Promise<ISqlTransaction> {
    const client = await this.pool.connect();
    await client.query("BEGIN");
    return new PgTransaction(client);
  }

  public end(): Promise<void> {
    return this.pool.end();
  }
}
