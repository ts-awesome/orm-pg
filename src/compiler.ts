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
  IExpr,
} from '@viatsyshyn/ts-orm';

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

  compileOrderExp(expr: {_column: string, _order: string} | {_column: string, _order: string}[]): string {
    if (Array.isArray(expr)) {
      return expr.map(item => this.compileOrderExp(item)).join(', ');
    }
    const {_column, _order} = expr;
    return `${pgBuilder.escapeColumn(_column)} ${_order}`;
  },

  // generic
  compileExp(expr: IExpr | IExpr[] | 'NULL' | '*' | any): string {
    if (Array.isArray(expr)) {
      return `(${expr.map(item => this.compileExp(item)).join(', ')})`;
    }
    if (expr === null) return 'NULL';
    if (expr === 'NULL') return 'NULL';
    if (expr === '*') return '*';

    const {_column, _func, _args, _operator, _operands} = expr as IExpr;

    if (_column) {
      return pgBuilder.escapeColumn(_column);
    }
    if (_func) {
      return `${_func}(${_args!.map((arg: any) => this.compileExp(arg)).join(', ')})`;
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

    let wrapper = undefined;
    if (typeof expr === 'object' && expr.value !== undefined) {
      wrapper = expr.wrapper;
      expr = expr.value;
    } else if (typeof expr === 'object') {
      throw new Error(`Property "value" is required, got ${JSON.stringify(expr)}`);
    }

    if (expr === null) {
      return 'NULL';
    }

    if (!this._paramMap.has(expr)) {
      this._paramMap.set(expr, this._paramCount++);
    }

    const value = `:${pgBuilder.getParam(this._paramMap.get(expr))}`;
    return wrapper ? wrapper(value) : value;
  },

  processColumns(columns?: (IExpr|string)[]) {
    if (!Array.isArray(columns) || columns.length < 1) {
      return '*'
    }

    return columns.map(column => {
      const {_alias, _operands} = column as IExpr;
      if (_alias) return `${this.compileExp(_operands)} AS ${pgBuilder.escapeColumn(_alias)}`;
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
    sql += ' ' + _joins.map(({_table, _condition, _type = 'INNER', _alias = null}: any) => {
      return `${_type} JOIN ${pgBuilder.escapeTable(_table)}${_alias ? ` AS ${pgBuilder.escapeTable(_alias)}` : ''} ON ${sqlCompiler.compileExp(_condition)}`;
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
    sql += ' ORDER BY ' + sqlCompiler.compileOrderExp(_orderBy)
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

  const keys = Object.keys(_values).filter(k => _values[k] !== undefined);
  const fields = keys.map(field => pgBuilder.escapeColumn(field));
  const values = keys.map(field => sqlCompiler.compileExp(_values[field]));

  let sql = `INSERT INTO ${pgBuilder.escapeTable(_table.tableName)} (${fields.join(', ')}) VALUES (${values.join(', ')})  RETURNING *;`;

  const params = sqlCompiler.collectParams();

  return {
    sql,
    params
  };
}

function UpsertCompiler({_values, _table, _conflictExp}: IBuildableUpsertQuery): ISqlQuery {
  sqlCompiler.resetParams();

  const keys = Object.keys(_values).filter(k => _values[k] !== undefined);
  const fields = keys.map(field => pgBuilder.escapeColumn(field));
  const values = keys.map(field => sqlCompiler.compileExp(_values[field]));
  const updateValues = keys.map(field => `${pgBuilder.escapeColumn(field)} = ${sqlCompiler.compileExp(_values[field])}`);

  let sql = `
    INSERT INTO ${pgBuilder.escapeTable(_table.tableName)} (${fields.join(', ')})
    VALUES (${values.join(', ')})
    ON CONFLICT`;

  if(!_conflictExp) {
    sql += ` DO NOTHING`;
  } else {
    sql += ` (${_conflictExp._columns.map((c: any) => pgBuilder.escapeColumn(c)).join(',')})`;
    if (Array.isArray(_conflictExp._where) && _conflictExp._where.length > 0) {
      sql += ` WHERE ${sqlCompiler.compileExp({
        _operator: 'AND',
        _operands: _conflictExp._where
      })}`
    }
    sql += ` DO UPDATE SET ${updateValues.join(', ')}`;
  }
  sql += ` RETURNING *;`;

  const params = sqlCompiler.collectParams();
  return {
    sql,
    params
  };
}

function UpdateCompiler({_values, _where, _table, _limit}: IBuildableUpdateQuery): ISqlQuery {
  sqlCompiler.resetParams();

  const values = Object
    .keys(_values)
    .filter(field => _values[field] !== undefined)
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
  sql += ` RETURNING *;`;

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
  sql += ` RETURNING *;`;

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
