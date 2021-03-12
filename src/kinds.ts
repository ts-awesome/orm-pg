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
