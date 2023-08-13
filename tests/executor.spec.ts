import 'reflect-metadata';
import { PgExecutor, DbError, FkViolatedDbError, DuplicateValueDbError } from '../dist';
import {ReaderError} from "@ts-awesome/model-reader";
import {NamedParameter} from "@ts-awesome/orm/dist/wrappers";

describe('Executor', () => {
  it('should include named params', async () => {
    const named = new NamedParameter('test')

    const executor = new PgExecutor({
      query(query) {
        expect(query.text).toBe('SELECT $1')
        expect(query.values).toStrictEqual([123])
        return []
      }
    } as any);

    executor.setNamedParameter(named, 123)
    await executor.execute({sql: `SELECT :test`});
  })

  it('should throw DbError', async () => {
    const executor = new PgExecutor({
      query(...args: any[]) {
        throw {
          code: '123',
          detail: 'ddd',
          error: 'error'
        }
      }
    } as any);

    expect.assertions(1);
    await expect(executor.execute({sql: 'SELECT 1'})).rejects
      .toStrictEqual(new DbError('123', undefined, 'ddd', 'error'));
  })

  it('should throw FkViolatedDbError', async () => {
    const executor = new PgExecutor({
      query(...args: any[]) {
        throw {
          code: '23503',
          detail: 'ddd',
          error: 'error'
        }
      }
    } as any);

    expect.assertions(1);
    await expect(executor.execute({sql: 'SELECT 1'})).rejects
      .toStrictEqual(new FkViolatedDbError('ddd', 'error'));
  })

  it('should throw DuplicateValueDbError', async () => {
    const executor = new PgExecutor({
      query(...args: any[]) {
        throw {
          code: '23505',
          detail: 'ddd (field) (value)',
          error: 'error',
          table: 'bbb'
        }
      }
    } as any);

    expect.assertions(1);
    await expect(executor.execute({sql: 'SELECT 1'})).rejects
      .toStrictEqual(new DuplicateValueDbError({detail: 'ddd (field) (value)', error: 'error', table: 'bbb'} as any));
  })

  it('should rethrow', async () => {
    const executor = new PgExecutor({
      query(...args: any[]) {
        throw new ReaderError('something went wrong')
      }
    } as any);

    expect.assertions(1);
    await expect(executor.execute({sql: 'SELECT 1'})).rejects
      .toStrictEqual(new ReaderError('something went wrong'));
  })
})
