import 'reflect-metadata';
import { PgCompiler } from '../dist';
import {
  alias,
  and, Delete,
  desc, IBuildableQuery,
  Insert, max, of,
  Select, Update, Upsert,
  TableRef, dbTable, dbField,
} from '@ts-awesome/orm';
import {readModelMeta} from '@ts-awesome/orm/dist/builder';
import { Employee, Person } from './models';

describe('Compiler', () => {
  const pgCompiler = new PgCompiler();
  const expectation = {sql: '', params: {}};
  const tableName = readModelMeta(Person).tableName;
  const empTableName = readModelMeta(Employee).tableName;
  const person: InstanceType<typeof Person> = {id: 1, name: 'Name', age: 18, city: 'City', uid: '123'};
  const limit = 10;
  const offset = 5;

  beforeEach(() => {
    expectation.sql = '';
    expectation.params = {};
  });

  describe('Select statement', () => {

    it('Group by', () => {
      const query = Select(Person).groupBy(['age']);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" GROUP BY ("${tableName}"."age")`;
      expect(result).toStrictEqual(expectation);
    });

    it('ASC Order by', () => {
      const query = Select(Person).orderBy(model => [model.age]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" ORDER BY "${tableName}"."age" ASC`;
      expect(result).toStrictEqual(expectation);
    });

    it('DESC Order by', () => {
      const query = Select(Person).orderBy(model => [desc(model.age)]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" ORDER BY "${tableName}"."age" DESC`;
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause', () => {
      const query = Select(Person).where(model => model.age.gte(person.age));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" WHERE (("${tableName}"."age" >= :p0))`;
      expectation.params = {p0: person.age};
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause with uid', () => {
      const query = Select(Person).where(model => model.uid.neq(person.uid));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" WHERE ((HEX("${tableName}"."uid") <> :p0))`;
      expectation.params = {p0: person.uid};
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause with date', () => {
      const now = new Date;
      const tableName = 'dated'
      @dbTable(tableName)
      class DatedModel {
        @dbField
        created!: Date;
      }

      const query = Select(DatedModel).where(model => model.created.neq(now));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" WHERE (("${tableName}"."created" <> :p0))`;
      expectation.params = {p0: now.toISOString()};
      expect(result).toStrictEqual(expectation);
    });

    it('SELECT uid column', () => {
      const query = Select(Person).columns(x => [x.uid]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL HEX("${tableName}"."uid") AS "uid" FROM "${tableName}"`;
      expectation.params = {};
      expect(result).toStrictEqual(expectation);
    });

    it('Having clause', () => {
      const [lowerBound, upperBound] = [18, 27];
      const query = Select(Person).groupBy(model => [model.city]).having(model => and(model.age.gte(lowerBound), model.age.lte(upperBound)));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" GROUP BY ("${tableName}"."city") HAVING ((("${tableName}"."age" >= :p0) AND ("${tableName}"."age" <= :p1)))`;
      expectation.params = {p0: person.age, p1: upperBound};
      expect(result).toStrictEqual(expectation);
    });

    it('Inner join', () => {
      const query = Select(Person).join(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" INNER JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Full outer join', () => {
      const query = Select(Person).joinFull(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" FULL OUTER JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Left join', () => {
      const query = Select(Person).joinLeft(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" LEFT JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Right join', () => {
      const query = Select(Person).joinRight(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" RIGHT JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Join with alias', () => {
      const tableRef = new TableRef(Employee);
      const query = Select(Person).join(
        Employee,
        tableRef,
        (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" INNER JOIN "Employee" AS "${tableRef.tableName}" ON ("${tableName}"."id" = "${tableRef.tableName}"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Limit', () => {
      const query = Select(Person).limit(limit);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" LIMIT :p0`;
      expectation.params = {p0: limit};
      expect(result).toStrictEqual(expectation);
    });

    it('Offset', () => {
      const query = Select(Person).offset(offset);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}".* FROM "${tableName}" OFFSET :p0`;
      expectation.params = {p0: offset};
      expect(result).toStrictEqual(expectation);
    });

    it('Columns list', () => {
      const query = Select(Person).columns(['name', 'age']);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."name", "${tableName}"."age" FROM "${tableName}"`;
      expect(result).toStrictEqual(expectation);
    });

    it('Columns with column builder', () => {
      const query = Select(Person).columns(model => [model.name, model.age]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."name", "${tableName}"."age" FROM "${tableName}"`;
      expect(result).toStrictEqual(expectation);
    });

    it('Columns with alias and function', () => {
      const query = Select(Person).columns(model => [alias(model.name, 'PersonAge'), max(model.age)]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL ("${tableName}"."name") AS "PersonAge", MAX("${tableName}"."age") FROM "${tableName}"`;
      expect(result).toStrictEqual(expectation);
    });

    it('Columns with alias and operator', () => {
      const ageAlias = 'AgePusFive';
      const additionalYears = 5;
      const query = Select(Person).columns(model => [alias(model.age.add(additionalYears), ageAlias)]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL (("${tableName}"."age" + :p0)) AS "${ageAlias}" FROM "${tableName}"`;
      expectation.params = {p0: additionalYears};
      expect(result).toStrictEqual(expectation);
    });
  });

  describe('INSERT statement', () => {

    it('Usual clause', () => {
      const query = Insert(Person).values(person);
      const result = pgCompiler.compile(query);
      expectation.sql = `INSERT INTO "${tableName}" ("id", "name", "age", "city", "uid") VALUES (:p0, :p1, :p2, :p3, UNHEX(:p4)) RETURNING *;`;
      expectation.params = {p0: person.id, p1: person.name, p2: person.age, p3: person.city, p4: person.uid};
      expect(result).toStrictEqual(expectation);
    });
  });

  describe('UPSERT statement', () => {

    it('Usual clause', () => {
      const query = Upsert(Person).values(person);
      const result = pgCompiler.compile(query);
      expectation.sql = `INSERT INTO "${tableName}" ("id", "name", "age", "city", "uid") VALUES (:p0, :p1, :p2, :p3, UNHEX(:p4)) ON CONFLICT DO NOTHING RETURNING *;`;
      expectation.params = {p0: person.id, p1: person.name, p2: person.age, p3: person.city, p4: person.uid};
      expect(result).toStrictEqual(expectation);
    });

    it('With conflict condition', () => {
      // How correctly use where clause in upsert statement?
      const query = Upsert(Person).values(person).conflict('idx').where(model => model.id.eq(person.id));
      const result = pgCompiler.compile(query);
      expectation.sql = `INSERT INTO "${tableName}" ("id", "name", "age", "city", "uid") VALUES (:p0, :p1, :p2, :p3, UNHEX(:p4)) ON CONFLICT ("id") DO UPDATE SET "id" = :p0, "name" = :p1, "age" = :p2, "city" = :p3, "uid" = UNHEX(:p4) RETURNING *;`;
      expectation.params = {p0: person.id, p1: person.name, p2: person.age, p3: person.city, p4: person.uid};
      expect(result).toStrictEqual(expectation);
    });
  });

  describe('UPDATE statement', () => {

    it('Usual clause', () => {
      const query = Update(Person).values(person);
      const result = pgCompiler.compile(query);
      expectation.sql = `UPDATE "${tableName}" SET "id" = :p0, "name" = :p1, "age" = :p2, "city" = :p3, "uid" = UNHEX(:p4) RETURNING *;`;
      expectation.params = {p0: person.id, p1: person.name, p2: person.age, p3: person.city, p4: person.uid};
      expect(result).toStrictEqual(expectation);
    });

    it('With where clause', () => {
      const query = Update(Person).values(person).where(model => model.id.eq(person.id));
      const result = pgCompiler.compile(query);
      expectation.sql = `UPDATE "${tableName}" SET "id" = :p0, "name" = :p1, "age" = :p2, "city" = :p3, "uid" = UNHEX(:p4) WHERE (("${tableName}"."id" = :p0)) RETURNING *;`;
      expectation.params = {p0: person.id, p1: person.name, p2: person.age, p3: person.city, p4: person.uid};
      expect(result).toStrictEqual(expectation);
    });

    it('With limitation', () => {
      const query = Update(Person).values(person).where(model => model.id.eq(person.id)).limit(limit);
      const result = pgCompiler.compile(query);
      expectation.sql = `UPDATE "${tableName}" SET "id" = :p0, "name" = :p1, "age" = :p2, "city" = :p3, "uid" = UNHEX(:p4) WHERE (("${tableName}"."id" = :p0)) LIMIT :p5 RETURNING *;`;
      expectation.params = {p0: person.id, p1: person.name, p2: person.age, p3: person.city, p4: person.uid, p5: limit};
      expect(result).toStrictEqual(expectation);
    });
  });

  describe('Delete statement', () => {

    it('Usual clause', () => {
      const query = Delete(Person).where(model => model.id.eq(person.id));
      const result = pgCompiler.compile(query);
      expectation.sql = `DELETE FROM "${tableName}" WHERE (("${tableName}"."id" = :p0)) RETURNING *;`;
      expectation.params = {p0: person.id};
      expect(result).toStrictEqual(expectation);
    });

    it('With limitation', () => {
      const query = Delete(Person).where(model => model.age.lt(person.age)).limit(limit);
      const result = pgCompiler.compile(query);
      expectation.sql = `DELETE FROM "${tableName}" WHERE (("${tableName}"."age" < :p0)) LIMIT :p1 RETURNING *;`;
      expectation.params = {p0: person.age, p1: limit};
      expect(result).toStrictEqual(expectation);
    });
  });

  it('Complex query', () => {
    const salary = 1000;
    const nameAlias = 'PersonName';
    const query = Select(Person)
      .columns(({name, age}) => [alias(name, nameAlias), age])
      .join(Employee, (person, employee) => person.id.eq(employee.personId))
      .where(({age, city}) => and(age.gte(person.age), city.like(person.city)))
      .where(() => of(Employee, 'salary').gte(salary))
      .groupBy(({id}) => [id]);
    const result = pgCompiler.compile(query);
    expectation.sql = `SELECT ALL ("${tableName}"."name") AS "${nameAlias}", "${tableName}"."age" FROM "${tableName}"` +
      ` INNER JOIN "${empTableName}" ON ("${tableName}"."id" = "${empTableName}"."personId")` +
      ` WHERE ((("${tableName}"."age" >= :p0) AND ("${tableName}"."city" LIKE :p1)) AND ("${empTableName}"."salary" >= :p2))` +
      ` GROUP BY ("${tableName}"."id")`;
    expectation.params = {p0: person.age, p1: person.city, p2: salary};
    expect(result).toStrictEqual(expectation);
  });

  it('Should fail on unsupported query type', () => {
    const query = {_type: 'WrongType'};
    expect(()=> {
      pgCompiler.compile(query as IBuildableQuery);
    }).toThrowError('Unsupported query ' + query);
  });
});
