import {PoolClient} from "pg";
import {PgExecutor} from "./executor";
import {ISqlTransaction} from "./interfaces";
import {injectable} from "inversify";

@injectable()
export class PgTransaction extends PgExecutor implements ISqlTransaction {
  private conn: PoolClient;
  private isFinished = false;

  constructor(conn: PoolClient) {
    super(conn);
    this.conn = conn;
  }

  public get finished(): boolean {
    return this.isFinished;
  }

  public async commit(): Promise<void> {
    if (this.finished) {
      throw new Error();
    }

    await this.conn.query("COMMIT");
    this.conn.release();
  }

  public async rollback(): Promise<void> {
    if (this.finished) {
      throw new Error();
    }

    await this.conn.query("ROLLBACK");
    this.conn.release();
  }
}
