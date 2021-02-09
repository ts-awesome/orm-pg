import { dbField, dbTable } from '@ts-awesome/orm';

@dbTable('Person', [{name: 'id', fields: ['id']}])
export class Person {
  @dbField({
    primaryKey: true,
    autoIncrement: true
  })
  id!: number;

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
