import {dbField, dbTable, IDbField} from '@ts-awesome/orm';

export const UID: IDbField = {
  reader(raw) {
    return typeof raw === 'string' ? raw.toLowerCase() : raw
  },
  writer(raw) {
    return typeof raw === 'string' ? raw.toLowerCase() : raw
  },
  readQuery(name) { return `HEX(${name})`},
  writeQuery(name) { return `UNHEX(${name})`},
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
