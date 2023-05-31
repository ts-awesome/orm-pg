import 'reflect-metadata';
import { PgCompiler } from '../dist';
import {
  alias,
  and, Delete,
  desc, IBuildableQuery,
  Insert, max, of,
  Select, Update, Upsert,
  TableRef, dbTable, dbField, exists, count,
} from '@ts-awesome/orm';
import {readModelMeta} from '@ts-awesome/orm/dist/builder';
import {Employee, EmployeeWithNames, Person, UID, PersonPrivate} from './models';

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
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" GROUP BY ("${tableName}"."age")`;
      expect(result).toStrictEqual(expectation);
    });

    it('ASC Order by', () => {
      const query = Select(Person).orderBy(model => [model.age]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" ORDER BY "${tableName}"."age" ASC`;
      expect(result).toStrictEqual(expectation);
    });

    it('DESC Order by', () => {
      const query = Select(Person).orderBy(model => [desc(model.age)]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" ORDER BY "${tableName}"."age" DESC`;
      expect(result).toStrictEqual(expectation);
    });

    it('DESC Order by anonymous field', () => {
      const query = Select(Person).orderBy(() => [desc(of(null, 'score'))]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" ORDER BY "score" DESC`;
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause', () => {
      const query = Select(Person).where(model => model.age.gte(person.age));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" WHERE (("${tableName}"."age" >= :p0))`;
      expectation.params = {p0: person.age};
      expect(result).toStrictEqual(expectation);
    });

    it('Encrypted Where clause', () => {
      const query = Select(PersonPrivate).where(model => model.medical.like('%something%'));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", (CAST("${tableName}"."email" AS Email)) AS "email", (Pgp_sym_decrypt("${tableName}"."medical", hmac(HEX("${tableName}"."uid"), :shared_key, 'sha256'))) AS "medical" FROM "${tableName}" WHERE ((Pgp_sym_decrypt("${tableName}"."medical", hmac(HEX("${tableName}"."uid"), :shared_key, 'sha256')) LIKE :p0))`;
      expectation.params = {p0: '%something%'};
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause with uid builder', () => {
      const query = Select(Person).where(model => model.uid.neq(person.uid));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" WHERE ((HEX("${tableName}"."uid") <> :p0))`;
      expectation.params = {p0: person.uid};
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause with uid literal', () => {
      const query = Select(Person).where({uid: person.uid});
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" WHERE (((HEX("${tableName}"."uid") = :p0)))`;
      expectation.params = {p0: person.uid};
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause with date builder', () => {
      const now = new Date;
      const tableName = 'dated'
      @dbTable(tableName)
      class DatedModel {
        @dbField
        created!: Date;
      }

      const query = Select(DatedModel).where(model => model.created.neq(now));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."created" FROM "${tableName}" WHERE (("${tableName}"."created" <> :p0))`;
      expectation.params = {p0: now.toISOString()};
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause with date literal', () => {
      const now = new Date;
      const tableName = 'dated'
      @dbTable(tableName)
      class DatedModel {
        @dbField
        created!: Date;
      }

      const query = Select(DatedModel).where({created: now});
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."created" FROM "${tableName}" WHERE ((("${tableName}"."created" = :p0)))`;
      expectation.params = {p0: now.toISOString()};
      expect(result).toStrictEqual(expectation);
    });

    it('subquery on self', () => {
      const query = Select(Person).where(({id}) => exists(Select(alias(Person, 'Person_a')).where(({id: _}) => _.eq(id))));

      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" WHERE (EXISTS (SELECT ALL "${tableName}_a"."id", (HEX("${tableName}_a"."uid")) AS "uid", "${tableName}_a"."name", "${tableName}_a"."age", "${tableName}_a"."city" FROM "${tableName}" AS "${tableName}_a" WHERE (("${tableName}_a"."id" = "${tableName}"."id"))))`;
      expectation.params = {};
      expect(result).toStrictEqual(expectation);
    })

    it('SELECT uid column with literal', () => {
      const query = Select(Person).columns(['uid']);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL (HEX("${tableName}"."uid")) AS "uid" FROM "${tableName}"`;
      expectation.params = {};
      expect(result).toStrictEqual(expectation);
    });

    it('SELECT uid column with builder', () => {
      const query = Select(Person).columns(x => [x.uid]);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL (HEX("${tableName}"."uid")) AS "uid" FROM "${tableName}"`;
      expectation.params = {};
      expect(result).toStrictEqual(expectation);
    });

    it('Having clause', () => {
      const [lowerBound, upperBound] = [18, 27];
      const query = Select(Person).groupBy(model => [model.city]).having(model => and(model.age.gte(lowerBound), model.age.lte(upperBound)));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" GROUP BY ("${tableName}"."city") HAVING ((("${tableName}"."age" >= :p0) AND ("${tableName}"."age" <= :p1)))`;
      expectation.params = {p0: person.age, p1: upperBound};
      expect(result).toStrictEqual(expectation);
    });

    it('Inner join', () => {
      const query = Select(Person).join(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" INNER JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Full outer join', () => {
      const query = Select(Person).joinFull(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" FULL OUTER JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Left join', () => {
      const query = Select(Person).joinLeft(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" LEFT JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Right join', () => {
      const query = Select(Person).joinRight(Employee, (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" RIGHT JOIN "Employee" ON ("${tableName}"."id" = "Employee"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Join with alias', () => {
      const tableRef = new TableRef(Employee);
      const query = Select(Person).join(
        Employee,
        tableRef,
        (root, other) => root.id.eq(other.personId));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" INNER JOIN "Employee" AS "${tableRef.tableName}" ON ("${tableName}"."id" = "${tableRef.tableName}"."personId")`;
      expect(result).toStrictEqual(expectation);
    });

    it('Limit', () => {
      const query = Select(Person).limit(limit);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" LIMIT :p0`;
      expectation.params = {p0: limit};
      expect(result).toStrictEqual(expectation);
    });

    it('Offset', () => {
      const query = Select(Person).offset(offset);
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."id", (HEX("${tableName}"."uid")) AS "uid", "${tableName}"."name", "${tableName}"."age", "${tableName}"."city" FROM "${tableName}" OFFSET :p0`;
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

    it('Where clause with same date multiple times', () => {
      const now = new Date;
      const tableName = 'dated'
      @dbTable(tableName)
      class DatedModel {
        @dbField
        created!: Date;
      }

      const query = Select(DatedModel).where(model => and(model.created.neq(now), model.created.neq(now), model.created.neq(now)));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."created" FROM "${tableName}" WHERE ((("${tableName}"."created" <> :p0) AND ("${tableName}"."created" <> :p0) AND ("${tableName}"."created" <> :p0)))`;
      expectation.params = {p0: now.toISOString()};
      expect(result).toStrictEqual(expectation);
    });

    it('Where clause with floats', () => {
      const tableName = 'floated'
      @dbTable(tableName)
      class DatedModel {
        @dbField
        value!: number;
      }

      const query = Select(DatedModel).where(model => and(model.value.gte(.5), model.value.neq(.7), model.value.mul(10).gte(50)));
      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."value" FROM "${tableName}" WHERE ((("${tableName}"."value" >= :p0) AND ("${tableName}"."value" <> :p1) AND (("${tableName}"."value" * :p2) >= :p3)))`;
      expectation.params = {p0: .5, p1: .7, p2: 10, p3: 50};
      expect(result).toStrictEqual(expectation);
    });

    it('where clause with duplicated params in subqueries', () => {
      @dbTable('actions')
      class PersonAction {
        @dbField public personId!: number;
        @dbField public action!: string;
        @dbField public created!: Date;
      }

      const ts = new Date(Date.now() - 3600);

      const query = Select(Person)
        .columns(({name}) => [
          name,
          alias(
            Select(PersonAction)
              .columns(() => [count()])
              .where(({personId, action, created}) => and(
                personId.eq(of(Person, 'id')),
                action.eq('a'),
                created.gte(ts)
              ))
              .asScalar()
              .mul(1).add(
              Select(PersonAction)
                .columns(() => [count()])
                .where(({personId, action, created}) => and(
                  personId.eq(of(Person, 'id')),
                  action.eq('b'),
                  created.gte(ts)
                ))
                .asScalar().mul(.5),
            ).add(
              Select(PersonAction)
                .columns(() => [count()])
                .where(({personId, action, created}) => and(
                  personId.eq(of(Person, 'id')),
                  action.eq('c'),
                  created.gte(ts)
                ))
                .asScalar().mul(.1),
            ),
            'score'
          )
        ]).orderBy(() => [desc(of(null, 'score'))]).limit(20);

      const result = pgCompiler.compile(query);
      expectation.sql = `SELECT ALL "${tableName}"."name", (((((SELECT ALL COUNT(*) FROM "actions" WHERE ((("actions"."personId" = "${tableName}"."id") AND ("actions"."action" = :p0) AND ("actions"."created" >= :p1)))) * :p2) + ((SELECT ALL COUNT(*) FROM "actions" WHERE ((("actions"."personId" = "${tableName}"."id") AND ("actions"."action" = :p3) AND ("actions"."created" >= :p1)))) * :p4)) + ((SELECT ALL COUNT(*) FROM "actions" WHERE ((("actions"."personId" = "${tableName}"."id") AND ("actions"."action" = :p5) AND ("actions"."created" >= :p1)))) * :p6))) AS "score" FROM "${tableName}" ORDER BY "score" DESC LIMIT :p7`;
      expectation.params = {p0: 'a', p1: ts.toISOString(), p2: 1, p3: 'b', p4: .5, p5: 'c', p6: .1, p7: 20};
      expect(result).toStrictEqual(expectation);
    })
  });

  describe('INSERT statement', () => {

    it('Usual clause', () => {
      const query = Insert(Person).values(person);
      const result = pgCompiler.compile(query);
      expectation.sql = `INSERT INTO "${tableName}" ("id", "name", "age", "city", "uid") VALUES (:p0, :p1, :p2, :p3, UNHEX(:p4)) RETURNING *;`;
      expectation.params = {p0: person.id, p1: person.name, p2: person.age, p3: person.city, p4: person.uid};
      expect(result).toStrictEqual(expectation);
    });

    it('All defaults', () => {
      @dbTable(tableName)
      class WithDefaults {
        @dbField({
          primaryKey: true,
          autoIncrement: true
        })
        id!: number;

        @dbField({
          kind: UID,
        })
        uid!: string;
      }

      const query = Insert(WithDefaults).values({});
      const result = pgCompiler.compile(query);
      expectation.sql = `INSERT INTO "${tableName}" DEFAULT VALUES RETURNING *;`;
      expectation.params = {};
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
