// noinspection JSUnusedGlobalSymbols

import {
  cast,
  DbValueType,
  IDbField,
  IFunctionCallOperation,
  IOperandable,
  Queryable
} from "@ts-awesome/orm";
import {NamedParameter, FunctionCall, Constant} from "@ts-awesome/orm/dist/wrappers";

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
  reader(value: DbValueType) {
    return typeof value === 'string' ? value.toLowerCase() : value
  },
  writer(value): DbValueType {
    return typeof value === 'string' ? value.toLowerCase() : value
  },
  readQuery(name: IOperandable<string>): IOperandable<string> {
    return cast(name as never, 'Email');
  },
  writeQuery(name: IOperandable<string>): IOperandable<string> {
    return cast(name as never, 'Email');
  }
}

function pgp_sym_encrypt(
  data: string | IOperandable<string>,
  key: string | IOperandable<string>,
  options = 'compress-algo=0, cipher-algo=aes256'
): IOperandable<string> & IFunctionCallOperation {
  return new FunctionCall('Pgp_sym_encrypt', [data, key, new Constant(options)]) as never;
}

function pgp_sym_decrypt(
  data: string | IOperandable<string>,
  key: string | IOperandable<string>
): IOperandable<string> & IFunctionCallOperation {
  return new FunctionCall('Pgp_sym_decrypt', [data, key]) as never;
}

// export function hmac(
//   data: string | IOperandable<string>,
//   key: string | IOperandable<string>,
//   algorithm = 'sha256'
// ): IOperandable<string> & IFunctionCallOperation {
//   return new FunctionCall('hmac', [data, key, new Constant(algorithm)]) as never;
// }

export const namedSharedKey = new NamedParameter('shared_key')

export const DB_ENCRYPTED_TEXT: IDbField = {
  readQuery(reference: IOperandable<string>): IOperandable<string> {
    return pgp_sym_decrypt(reference, namedSharedKey);
  },
  writeQuery(value: IOperandable<string>): IOperandable<string> {
    return pgp_sym_encrypt(value, namedSharedKey);
  }
};
export const DB_ENCRYPTED_JSON: IDbField = {
  ...DB_ENCRYPTED_TEXT,
  ...DB_JSON,
};

export const DB_ENCRYPTED_EMAIL: IDbField = {
  ...DB_EMAIL,
  readQuery(reference: IOperandable<string>, table: Queryable<{uid: string}>): IOperandable<string> {
    return DB_EMAIL.readQuery(DB_ENCRYPTED_TEXT.readQuery(reference, table), table);
  },
  writeQuery(value: IOperandable<string>, table: Queryable<{uid: string}>): IOperandable<string> {
    return DB_ENCRYPTED_TEXT.writeQuery(DB_EMAIL.writeQuery(value, table), table);
  }
};
