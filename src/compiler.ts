import {
  IBuildableQuery,
  IBuildableQueryCompiler,
  IBuildableSelectQuery,
  IBuildableInsertQuery,
  IBuildableUpdateQuery,
  IBuildableDeleteQuery,
  IBuildableUpsertQuery,
  IBuildableSubSelectQuery,
  DbValueType,
} from '@viatsyshyn/ts-orm';
import {IExpr} from "@viatsyshyn/ts-orm/dist/src/builder";

import {ISqlQuery} from './interfaces';
import {injectable} from "inversify";

const pgBuilder = {
  // pg specific
  escapeColumn(column: string) {
    if (column.indexOf('.')) {
      return column.split('.').map(x => `"${x}"`).join('.')
    }
    return `"${column}"`;
  },
  escapeTable(table: string) {
    return `"${table}"`
  },
  getParam(id: number) {
    return `p${id}`;
  },
};

const sqlCompiler = {
  _paramCount: 0,
  _paramMap: new Map<DbValueType, number>(),

  // generic
  compileExp(expr: IExpr | IExpr[] | 'NULL' | '*' | any): string {
    if (Array.isArray(expr)) {
      return `(${expr.map(item => this.compileExp(item)).join(', ')})`;
    }
    if (expr === 'NULL') return 'NULL';
    if (expr === '*') return '*';

    const {_column, _func, _args, _operator, _operands} = expr as IExpr;
    if (_column) {
      return pgBuilder.escapeColumn(_column);
    }
    if (_func) {
      return `${_func}(${_args!.map(arg => this.compileExp(arg)).join(', ')})`;
    }
    if (_operator) {
      switch (_operator) {
        case 'NOT':
          return `NOT (${this.compileExp(_operands![0])})`;
        case 'ANY':
        case 'ALL':
        case 'EXISTS':
          if ((_operands![0] as IBuildableSubSelectQuery)._table) {
            return `${_operator} (${SubSelectBuilder(_operands![0] as IBuildableSubSelectQuery)})`;
          } else {
            return `${_operator} (${this.compileExp(_operands![0])})`;
          }
        case 'SUBQUERY':
          return `(${SubSelectBuilder(_operands![0] as IBuildableSubSelectQuery)})`;
        case 'BETWEEN':
          return `(${this.compileExp(_operands![0])} BETWEEN ${this.compileExp(_operands![1])} AND ${this.compileExp(_operands![2])})`;
        default:
          return `(${(_operands! as IExpr[]).map(operand => this.compileExp(operand)).join(` ${_operator} `)})`;
      }
    }

    if (!this._paramMap.has(expr)) {
      this._paramMap.set(expr, this._paramCount++);
    }
    return `:${pgBuilder.getParam(this._paramMap.get(expr))}`;
  },

  processColumns(columns?: (IExpr|string)[]) {
    if (!Array.isArray(columns) || columns.length < 1) {
      return '*'
    }

    return columns.map(column => {
      const {_alias, _operands} = column as IExpr;
      if (_alias) return `${this.compileExp(_operands)} AS ${_alias}`;
      return typeof column === 'string' ? column : this.compileExp(column);
    }).join(', ')
  },

  resetParams(): void {
    this._paramMap = new Map();
    this._paramCount = 0;
  },

  collectParams(): {[key: string]: DbValueType} {
    const params: {[key: string]: DbValueType} = {};
    sqlCompiler._paramMap.forEach((value, key) => {
      params[pgBuilder.getParam(value)] = key;
    });
    return params;
  }
};

function SubSelectBuilder({_columns, _table, _where, _groupBy, _having, _joins}: IBuildableSubSelectQuery): string {
  let sql = `SELECT ${sqlCompiler.processColumns(_columns)} FROM ${pgBuilder.escapeTable(_table.tableName)}`;

  if (Array.isArray(_joins) && _joins.length) {
    sql += ' ' + _joins.map(({_table, _condition, _type = 'INNER'}: any) => {
      return `${_type} JOIN ${pgBuilder.escapeTable(_table)} ON ${sqlCompiler.compileExp(_condition)}`;
    }).join (' ');
  }

  if (Array.isArray(_where) && _where.length) {
    sql += ' WHERE ' + sqlCompiler.compileExp({
      _operator: 'AND',
      _operands: _where
    })
  }

  if (Array.isArray(_groupBy) && _groupBy.length) {
    sql += ' GROUP BY ' + sqlCompiler.compileExp(_groupBy)
  }

  if (Array.isArray(_having) && _having.length) {
    sql += ' HAVING ' + sqlCompiler.compileExp(_having)
  }

  return sql;
}

function SelectCompiler(query: IBuildableSelectQuery): ISqlQuery {
  sqlCompiler.resetParams();

  let sql = SubSelectBuilder(query);

  const {_orderBy, _limit, _offset} = query;
  if (Array.isArray(_orderBy) && _orderBy.length) {
    sql += ' ORDER BY ' + sqlCompiler.compileExp(_orderBy)
  }

  if (_limit) {
    sql += ' LIMIT ' + sqlCompiler.compileExp(_limit)
  }

  if (_offset) {
    sql += ' OFFSET ' + sqlCompiler.compileExp(_offset)
  }

  const params = sqlCompiler.collectParams();

  return {
    sql,
    params
  };
}

function InsertCompiler({_values, _table}: IBuildableInsertQuery): ISqlQuery {
  sqlCompiler.resetParams();

  const fields = Object.keys(_values).map(field => pgBuilder.escapeColumn(field));
  const values = Object.keys(_values).map(field => sqlCompiler.compileExp(_values[field]));

  let sql = `INSERT INTO ${pgBuilder.escapeTable(_table.tableName)} (${fields.join(', ')}) VALUES (${values.join(', ')})`;

  const params = sqlCompiler.collectParams();

  return {
    sql,
    params
  };
}

function UpsertCompiler(query: IBuildableUpsertQuery): ISqlQuery {
  return {
    sql: ''
  }
}

function UpdateCompiler({_values, _where, _table, _limit}: IBuildableUpdateQuery): ISqlQuery {
  sqlCompiler.resetParams();

  const values = Object
    .keys(_values)
    .map(field => `${pgBuilder.escapeColumn(field)} = ${sqlCompiler.compileExp(_values[field])}`);

  let sql = `UPDATE ${pgBuilder.escapeTable(_table.tableName)} SET ${values.join(', ')}`;

  if (Array.isArray(_where) && _where.length > 0) {
    sql += ` WHERE ${sqlCompiler.compileExp({
      _operator: 'AND',
      _operands: _where
    })}`;
  }

  if (_limit) {
    sql += ' LIMIT ' + sqlCompiler.compileExp(_limit)
  }

  const params = sqlCompiler.collectParams();

  return {
    sql,
    params
  };
}

function DeleteCompiler({_where, _table, _limit}: IBuildableDeleteQuery): ISqlQuery {
  sqlCompiler.resetParams();

  let sql = `DELETE FROM ${pgBuilder.escapeTable(_table.tableName)}`;

  if (Array.isArray(_where) && _where.length > 0) {
    sql += ` WHERE ${sqlCompiler.compileExp({
      _operator: 'AND',
      _operands: _where
    })}`;
  }

  if (_limit) {
    sql += ' LIMIT ' + sqlCompiler.compileExp(_limit)
  }

  const params = sqlCompiler.collectParams();

  return {
    sql,
    params
  };
}

@injectable()
export class PgCompiler implements IBuildableQueryCompiler<ISqlQuery> {
  compile(query: IBuildableQuery): ISqlQuery {
    switch (query._type) {
      case 'SELECT': return SelectCompiler(query);
      case 'INSERT': return InsertCompiler(query);
      case 'UPSERT': return UpsertCompiler(query);
      case 'UPDATE': return UpdateCompiler(query);
      case 'DELETE': return DeleteCompiler(query);
      default: throw new Error('Unsupported query ' + query);
    }
  }
}
