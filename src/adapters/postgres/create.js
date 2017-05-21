const R = require('ramda');
const { ident } = require('pg-format');
const { formatFile } = require('../../lib/format');
const { join } = require('path');
const createIndexFile = join(__dirname, '..', '..', 'sql/create-index.sql');

const DEFAULT_TABLE = { schema: '', table: '', primaryKey: '', columns: [], indexes: []};
const DEFAULT_COLUMN = { name: '', type: '', nullable: true, default: undefined };

const TYPE_MAP = {
  INT: 'INT',
  SMALLINT: 'SMALLINT',
  BIGINT: 'BIGINT',
  VARCHAR: 'VARCHAR',
  CHAR: 'CHAR',
  BOOLEAN: 'BOOLEAN',
  BOOL: 'BOOLEAN',
  TIMESTAMP: 'TIMESTAMP',
  TIMESTAMPTZ: 'TIMESTAMPTZ',
  DATE: 'DATE',
  TEXT: 'TEXT',
  FLOAT4: 'FLOAT4',
  FLOAT8: 'FLOAT8',
  BIGSERIAL: 'BIGSERIAL',
};

const generateCreate = ({ schema, table }) => [ `CREATE TABLE IF NOT EXISTS ${ident(schema)}.${ident(table)}` ];

const _generateColumn = (column) => {
  column.type = TYPE_MAP[column.type.toUpperCase()];

  const { name, type, length, nullable, default: defaultValue } = column;

  const len = length ? `(${length})` : '';
  const notNull = nullable ? '' : ' NOT NULL';
  const def = defaultValue !== undefined ? ` DEFAULT ${defaultValue}` : '';

  return `${ident(name)} ${type}${len}${notNull}${def}`;
}

const generateColumns = ({ columns }) => columns.map(R.bind(_generateColumn, this));

const generatePrimaryKey = ({ primaryKey }) => primaryKey ? `,PRIMARY KEY(${ident(primaryKey)})` : '';

const generateIndex = R.curry((schema, table, columns) => {
  const bits = R.flatten([ table, columns, 'idx']);
  const indexName = bits.join('_');
  return formatFile(createIndexFile, {schema, table, indexName, columns });
});

const generateIndexes = ({ schema, table, indexes }) => R.map(generateIndex(schema, table), indexes);

const translate = (table) => {

  const normaliseTuple = ([ field, value ]) => {
    const fnByField = {
      columns: R.map(R.merge(DEFAULT_COLUMN)),
      default: R.identity
    };
    const transformation = fnByField[field] || fnByField['default'];
    return R.set(R.lensProp(field), transformation(value), {});
  };

  const normalised = R.pipe(
    R.merge(DEFAULT_TABLE),
    R.toPairs,
    R.map(normaliseTuple),
    R.mergeAll
  );

  const normalisedTable = normalised(table);

  const [ createStatement, columnsStatement, pKeyStatement, indexesStatement ] = R.map((fn) => fn(normalisedTable), [
    generateCreate,
    generateColumns,
    generatePrimaryKey,
    generateIndexes
  ]);

  const hasContent = R.complement(R.isEmpty);
  const polish = R.pipe(
    R.filter(R.identity),
    R.join('\n')
  );

  return polish([
    ...createStatement,
    hasContent(columnsStatement) ? '(' : '',
    columnsStatement.join(',\n'),
    pKeyStatement,
    hasContent(columnsStatement) ? ')' : '',
    hasContent(columnsStatement) ? ';' : '',
    ...indexesStatement,
  ]);
};

module.exports = translate;
