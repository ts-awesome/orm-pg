# @ts-awesome/orm-pg

TypeScript ORM PostgreSQL driver for [@ts-awesome/orm](https://github.com/ts-awesome/orm)

Key features:

* uses [yesql](https://github.com/pihvi/yesql#readme) for named params
* can run raw SQL or compile IBuildableQuery


## Standalone

```ts
import {Pool, PoolConfig} from 'pg';
import {IBuildableQuery, IQueryExecutor, Select} from "@ts-awesome/orm";
import {ISqlQuery, PgCompiler, PgDriver} from "@ts-awesome/orm-pg"; 

const config: PoolConfig;
const pgPool = new Pool(config);

const driver = new PgDriver(pgPool);

const compiled: ISqlQuery = {
  // driver uses yesql
  sql: 'SELECT :value',
  params: {
    value: 1
  }
};
const results = await driver.execute(compiled);
```

## Vanilla use with ORM

```ts
import {Pool, PoolConfig} from 'pg';
import {IBuildableQuery, IQueryExecutor, Select} from "@ts-awesome/orm";
import {ISqlQuery, PgCompiler, PgDriver} from "@ts-awesome/orm-pg"; 

const config: PoolConfig;
const pgPool = new Pool(config);

const compiler = new PgCompiler();
const driver = new PgDriver(pgPool);

const query: IBuildableQuery;
const compiled: ISqlQuery = compiler.compile(query);
const results = await driver.execute(compiled);
```

## Use with IoC container

```ts
import {Pool, PoolConfig} from 'pg';
import {IBuildableQuery, IQueryExecutor, Select} from "@ts-awesome/orm";
import {ISqlQuery, PgCompiler, PgDriver} from "@ts-awesome/orm-pg"; 

const config: PoolConfig;
const pgPool = new Pool(config);

const compiler = new PgCompiler();
const driver = new PgDriver(pgPool);

const container: Container;

container.bind<IQueryDriver<ISqlQuery>>(SqlQueryDriverSymbol)
  .toDynamicValue(() => new PgDriver(pool))

container.bind<IBuildableQueryCompiler<ISqlQuery>>(SqlQueryBuildableQueryCompilerSymbol)
  .to(PgCompiler)
```

## Kinds

This package provides ORM kinds for: 

### DB_UID

This kind a dummy, but other drivers may have different behaviour

### DB_JSON 

This kind stringifies before write and parses raw value from DB. 
DB type should be TEXT or equivalent

### DB_EMAIL

This kind ensures DB fields is case-insensitive, also makes value lowercase on read/write
For more details check [here](https://dba.stackexchange.com/questions/68266/what-is-the-best-way-to-store-an-email-address-in-postgresql)

Depends on `citext` extension. 

Please run following initialization code on your DB

```sql
CREATE EXTENSION citext;
CREATE DOMAIN Email AS citext CHECK ( value ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$' );
```
