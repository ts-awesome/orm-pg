import {DbValueType, IDbField} from "@ts-awesome/orm";

export const DB_UID: IDbField = {};

export const DB_JSON: IDbField = {
  reader(raw: DbValueType): any {
    return typeof raw === 'string' ? JSON.parse(raw) : raw;
  },
  writer(value: any): DbValueType {
    return JSON.stringify(value);
  }
};

/**
 * requires following DDL
 * CREATE EXTENSION citext;
 * CREATE DOMAIN Email AS citext CHECK ( value ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$' );
 *
 * @link https://dba.stackexchange.com/questions/68266/what-is-the-best-way-to-store-an-email-address-in-postgresql
 */
export const DB_EMAIL: IDbField = {
  reader(value): DbValueType {
    return typeof value === 'string' ? value.toLowerCase() : value
  },
  writer(value): DbValueType {
    return typeof value === 'string' ? value.toLowerCase() : value
  },
  readQuery(name: string): string {
    return `${name}::Email`
  },
  writeQuery(name: string): string {
    return `${name}::Email`
  }
}
