import {Pool} from 'pg';

import {DbValueType, IQueryData, IQueryDriver, IsolationLevel, ITransaction} from '@ts-awesome/orm';
import {isSupportedIsolationLevel, PgTransaction, UNSUPPORTED_ISOLATION_LEVEL} from './transaction';
import {injectable, unmanaged} from 'inversify';
import { ISqlQuery } from './interfaces';
import {BaseDriver} from "@ts-awesome/orm/dist/base";
import {injectQueryParams, PgExecutor} from "./executor";
import {WithParams} from "@ts-awesome/orm/dist/interfaces";
import {buildContextQuery} from "./compiler";

const BEGIN = 'BEGIN TRANSACTION';

type PgTransactionalExecutor = Pick<Pool, 'query' | 'connect' | 'end'>;

@injectable()
export class PgDriver extends BaseDriver<ISqlQuery> implements IQueryDriver<ISqlQuery> {

  private executor: PgExecutor

  constructor(
    @unmanaged() protected readonly pool: PgTransactionalExecutor,
    @unmanaged() protected readonly context?: Readonly<Record<string, DbValueType>>,
  ) {
    super();
    this.executor = new PgExecutor(pool)
  }

  protected async do(query: ISqlQuery & WithParams): Promise<readonly IQueryData[]> {
    if (Object.keys(this.context ?? {}).length < 1) {
      return this.executor.execute(query);
    }

    const client = await this.pool.connect();

    try {
      const ctx = buildContextQuery('SESSION', this.context);
      await client.query(injectQueryParams(ctx));
      return new PgExecutor(client).execute(query);
    } finally {
      client.release();
    }
  }

  protected async startTransaction(isolationLevel?: IsolationLevel): Promise<ITransaction<ISqlQuery>> {
    const client = await this.pool.connect();

    if (isolationLevel && !isSupportedIsolationLevel(isolationLevel as string)) {
      throw new Error(UNSUPPORTED_ISOLATION_LEVEL + JSON.stringify(isolationLevel))
    }

    await client.query(BEGIN + (isSupportedIsolationLevel(isolationLevel) ? ' ISOLATION LEVEL ' + isolationLevel : ''));

    if (Object.keys(this.context ?? {}).length) {
      const query = buildContextQuery('LOCAL', this.context);
      await client.query(injectQueryParams(query));
    }

    return new PgTransaction(client);
  }

  public end(): Promise<void> {
    return this.pool.end();
  }
}
