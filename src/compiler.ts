import {
  IBuildableQuery,
  IBuildableSelectQuery,
  IBuildableInsertQuery,
  IBuildableUpdateQuery,
  IBuildableDeleteQuery,
  IBuildableUpsertQuery,
  IBuildableSubSelectQuery,
  DbValueType,
  IExpr,
  IColumnRef,
  IJoin,
} from '@ts-awesome/orm';

import {
  IExpression
} from "@ts-awesome/orm/dist/intermediate";

import {ISqlQuery} from './interfaces';
import {injectable} from "inversify";
import {BaseCompiler} from "@ts-awesome/orm/dist/base";

const pgBuilder = {
  // pg specific
  escapeColumnRef(column: IColumnRef) {
    const {name, table, wrapper} = column;
    let value = pgBuilder.escapeColumnName(name);
    if (table) {
      value = `${pgBuilder.escapeTable(table)}.${value}`;
    }
    if (typeof wrapper === 'function') {
      value = wrapper(value);
    }
    return value;
  },
  escapeColumnName(name: string) {
    return `"${name}"`;
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

  compileOrderExp(expr: {_column: IColumnRef, _order?: string} | {_column: IColumnRef, _order?: string}[]): string {
    if (Array.isArray(expr)) {
      return expr.map(item => this.compileOrderExp(item)).join(', ');
    }
    const {_column, _order} = expr;
    return `${pgBuilder.escapeColumnRef(_column)} ${_order ?? 'ASC'}`;
  },

  // generic
  compileExp(expr: IExpression | IExpression[] | any): string {
    if (Array.isArray(expr)) {
      return `(${expr.map(item => this.compileExp(item)).join(', ')})`;
    }
    if (expr === null) return 'NULL';
    if (expr === 'NULL') return 'NULL';
    if (expr === '*') return '*';

    const {_column, _func, _args, _operator, _operands} = expr as {
      _column?: IColumnRef,
      _func?: string,
      _args?: IExpression[],
      _operator?: string,
      _operands?: IExpression[],
    };

    if (_column) {
      return pgBuilder.escapeColumnRef(_column);
    }
    if (_func && _args) {
      return `${_func}(${_args.map((arg: any) => this.compileExp(arg)).join(', ')})`;
    }
    if (_operator && _operands) {
      // noinspection FallThroughInSwitchStatementJS
      switch (_operator) {
        case 'NOT':
          return `NOT (${this.compileExp(_operands[0])})`;
        case 'ANY':
        case 'ALL':
        case 'EXISTS':
          if ((_operands[0] as IBuildableSubSelectQuery)._table) {
            return `${_operator} (${SubSelectBuilder(_operands[0] as IBuildableSubSelectQuery)})`;
          } else {
            return `${_operator} (${this.compileExp(_operands[0])})`;
          }
        case 'SUBQUERY':
          return `(${SubSelectBuilder(_operands[0] as IBuildableSubSelectQuery)})`;
        case 'BETWEEN':
          return `(${this.compileExp(_operands[0])} BETWEEN ${this.compileExp(_operands[1])} AND ${this.compileExp(_operands[2])})`;
        case 'IN': {
          const ops = _operands as IExpr[];
          if (ops.length === 2 && Array.isArray(ops[1])) {
            const values = ops[1] as any[];
            if (values.length === 0) {
              return `(TRUE = FALSE)`;
            }
          }
        }
        // eslint-disable-next-line no-fallthrough
        default:
          return `(${(_operands as IExpression[]).map(operand => this.compileExp(operand)).join(` ${_operator} `)})`;
      }
    }

    let wrapper = undefined;
    if (typeof expr === 'object' && expr.value !== undefined) {
      wrapper = expr.wrapper;
      expr = expr.value;
    } else if (expr instanceof Date) {
      expr = expr.toISOString();
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

  processColumns(tableName: string, columns?: (IExpression|string)[]) {
    if (!Array.isArray(columns) || columns.length < 1) {
      return pgBuilder.escapeTable(tableName) + '.*';
    }

    return columns.map((column, idx) => {
      const {_alias, _operands, _column} = column as any as {_alias: string; _operands: IExpression[], _column: IColumnRef};
      if (_alias) {
        return `${this.compileExp(_operands)} AS ${pgBuilder.escapeColumnName(_alias)}`;
      }
      if (typeof _column?.name === 'string' && _column.wrapper != null) {
        return `${this.compileExp(column)} AS ${pgBuilder.escapeColumnName(_column?.name)}`;
      }
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

function SubSelectBuilder({_columns, _distinct, _table, _where, _groupBy, _having, _joins, _alias}: IBuildableSubSelectQuery): string {
  const sql = [
    'SELECT',
    _distinct === true ? 'DISTINCT' : 'ALL',
    sqlCompiler.processColumns(_alias || _table.tableName, _columns),
    'FROM',
    pgBuilder.escapeTable(_table.tableName),
    _alias ? `AS ${pgBuilder.escapeTable(_alias)}` : '',
  ];

  if (Array.isArray(_joins) && _joins.length) {
    for(const {_tableName, _condition, _type = 'INNER', _alias} of _joins as IJoin[]) {
      sql.push(
        _type,
        'JOIN',
        pgBuilder.escapeTable(_tableName),
        _alias ? `AS ${pgBuilder.escapeTable(_alias)}` : '',
        'ON',
        sqlCompiler.compileExp(_condition),
      );
    }
  }

  if (Array.isArray(_where) && _where.length) {
    sql.push('WHERE', sqlCompiler.compileExp({
      _operator: 'AND',
      _operands: _where
    }));
  }

  if (Array.isArray(_groupBy) && _groupBy.length) {
    sql.push('GROUP BY', sqlCompiler.compileExp(_groupBy));
  }

  if (Array.isArray(_having) && _having.length) {
    sql.push('HAVING', sqlCompiler.compileExp(_having));
  }

  return sql.filter(x => x).join(' ');
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
  const fields = keys.map(field => pgBuilder.escapeColumnName(field));

  // ALL DEFAULT VALUES special case
  if (fields.length === 0) {
    return {
      sql: `INSERT INTO ${pgBuilder.escapeTable(_table.tableName)} DEFAULT VALUES RETURNING *;`,
      params: {}
    };
  }

  const values = keys.map(field => sqlCompiler.compileExp(_values[field]));

  const sql = `INSERT INTO ${pgBuilder.escapeTable(_table.tableName)} (${fields.join(', ')}) VALUES (${values.join(', ')}) RETURNING *;`;

  const params = sqlCompiler.collectParams();

  return {
    sql,
    params
  };
}

function UpsertCompiler({_values, _table, _conflictExp}: IBuildableUpsertQuery): ISqlQuery {
  sqlCompiler.resetParams();

  const keys = Object.keys(_values).filter(k => _values[k] !== undefined);
  const fields = keys.map(field => pgBuilder.escapeColumnName(field));
  const values = keys.map(field => sqlCompiler.compileExp(_values[field]));
  const updateValues = keys.map(field => `${pgBuilder.escapeColumnName(field)} = ${sqlCompiler.compileExp(_values[field])}`);

  let sql = `INSERT INTO ${pgBuilder.escapeTable(_table.tableName)} (${fields.join(', ')})` +
    ` VALUES (${values.join(', ')})` +
    ` ON CONFLICT`;

  if(!_conflictExp) {
    sql += ` DO NOTHING`;
  } else {
    sql += ` (${_conflictExp._columns.map((c: any) => pgBuilder.escapeColumnName(c)).join(',')})`;
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
    .map(field => `${pgBuilder.escapeColumnName(field)} = ${sqlCompiler.compileExp(_values[field])}`);

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
export class PgCompiler extends BaseCompiler<ISqlQuery> {
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
