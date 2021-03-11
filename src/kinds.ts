import {DbValueType, IDbField} from "@ts-awesome/orm";

export const UID: IDbField = {
  readQuery(name) {
    return `HEX(${name})`;
  },
  writeQuery(name) {
    return `UNHEX(${name})`;
  },
};

export const json: IDbField = {
  reader(raw: DbValueType): any {
    return typeof raw === 'string' ? JSON.parse(raw) : raw;
  },
  writer(value: any): DbValueType {
    return JSON.stringify(value);
  }
};
