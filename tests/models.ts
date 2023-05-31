import {dbField, dbTable, IDbField} from '@ts-awesome/orm';
import {FunctionCall} from "@ts-awesome/orm/dist/wrappers";
import {DB_ENCRYPTED_TEXT, DB_EMAIL} from "../dist";

export const UID: IDbField = {
  reader(raw) {
    return typeof raw === 'string' ? raw.toLowerCase() : raw
  },
  writer(raw) {
    return typeof raw === 'string' ? raw.toLowerCase() : raw
  },
  readQuery(name) { return new FunctionCall('HEX', [name]) },
  writeQuery(name) { return new FunctionCall('UNHEX', [name]) },
};

@dbTable('Person', [{name: 'idx', fields: ['id']}])
export class Person {
  @dbField({
    primaryKey: true,
    autoIncrement: true
  })
  id!: number;

  @dbField({
    kind: UID,
  })
  uid!: string;

  @dbField
  name!: string;

  @dbField
  age!: number;

  @dbField
  city!: string;
}

@dbTable('Person', [{name: 'idx', fields: ['id']}])
export class PersonPrivate {
  @dbField({
    primaryKey: true,
    autoIncrement: true
  })
  id!: number;

  @dbField({
    kind: UID,
  })
  uid!: string;

  @dbField({
    kind: DB_EMAIL,
  })
  email!: string;

  @dbField({kind: DB_ENCRYPTED_TEXT})
  medical!: string;
}

@dbTable('Employee')
export class Employee {
  @dbField(
    {
      primaryKey: true,
      autoIncrement: true
    }
  )
  id!: number;

  @dbField
  personId!: number;

  @dbField
  company!: string;

  @dbField
  salary!: number;
}

@dbTable('employee')
export class EmployeeWithNames {
  @dbField(
    {
      primaryKey: true,
      autoIncrement: true
    }
  )
  id!: number;

  @dbField('person_id')
  personId!: number;

  @dbField('company_id')
  companyId!: number;

  @dbField
  salary!: number;
}

