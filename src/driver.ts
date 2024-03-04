import {Pool} from 'pg';

import {IQueryData, IQueryDriver, IsolationLevel, ITransaction} from '@ts-awesome/orm';
import {isSupportedIsolationLevel, PgTransaction, UNSUPPORTED_ISOLATION_LEVEL} from './transaction';
import {injectable, unmanaged} from 'inversify';
import { ISqlQuery } from './interfaces';
import {BaseDriver} from "@ts-awesome/orm/dist/base";
import {PgExecutor} from "./executor";
import {WithParams} from "@ts-awesome/orm/dist/interfaces";

const BEGIN = 'BEGIN TRANSACTION';

type PgTransactionalExecutor = Pick<Pool, 'query' | 'connect' | 'end'>;

@injectable()
export class PgDriver extends BaseDriver<ISqlQuery> implements IQueryDriver<ISqlQuery> {

  private executor: PgExecutor

  constructor(
    @unmanaged() private readonly pool: PgTransactionalExecutor
  ) {
    super();
    this.executor = new PgExecutor(pool)
  }

  protected do(query: ISqlQuery & WithParams): Promise<readonly IQueryData[]> {
    return this.executor.execute(query);
  }

  protected async startTransaction(isolationLevel?: IsolationLevel): Promise<ITransaction<ISqlQuery>> {
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
