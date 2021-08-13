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

export const DB_EMAIL: IDbField = {
  reader(raw: DbValueType): DbValueType {
    return typeof raw === 'string' ? raw.toLowerCase() : raw
  },
  writer(value): DbValueType {
    return typeof value === 'string' ? value.toLowerCase() : value
  },
  readQuery(name: string): string {
    return `lower(${name})`
  },
  writeQuery(name: string): string {
    return `lower(${name})`
  }
}
