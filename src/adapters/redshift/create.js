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

const polish = R.pipe(
  R.filter(R.identity),
  R.join('\n')
);

const generateCreate = ({ schema, table }) => [ `CREATE TABLE IF NOT EXISTS ${ident(schema)}.${ident(table)}` ];

const generateColumn = ({ name, type, length, nullable, encode, default: defaultValue }) => {
  const colType = TYPE_MAP[type.toUpperCase()];
  const len = length ? `(${length})` : '';
  const notNull = nullable ? '' : ' NOT NULL';
  const def = defaultValue !== undefined ? ` DEFAULT ${defaultValue}` : '';
  const encodeValue = encode ? ` ENCODE ${encode}` : '';
  return `${ident(name)} ${colType}${len}${notNull}${def}${encodeValue}`;
}

const generateTableOptions = ({ distStyle, distKey, sortKey }) =>
  polish([
    distStyle ? `DISTSTYLE ${distStyle}` : '',
    distKey ? `DISTKEY(${distKey})` : '',
    sortKey ? `SORTKEY(${sortKey})` : '',
  ]);

const generateColumns = ({ columns }) => R.map(generateColumn, columns);

const generatePrimaryKey = ({ primaryKey }) => primaryKey ? `,PRIMARY KEY(${ident(primaryKey)})` : '';

const generateIndex = R.curry((schema, table, columns) => {
  const indexName = R.join('_', R.flatten([ table, columns, 'idx']));
  return formatFile(createIndexFile, { schema, table, indexName, columns });
});

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

  const [
    createStatement,
    columnsStatement,
    pKeyStatement,
    optionsStatement,
  ] = R.map((fn) => fn(normalisedTable), [
    generateCreate,
    generateColumns,
    generatePrimaryKey,
    generateTableOptions
  ]);

  const hasContent = R.complement(R.isEmpty);

  return polish([
    ...createStatement,
    hasContent(columnsStatement) ? '(' : '',
    columnsStatement.join(',\n'),
    pKeyStatement,
    hasContent(columnsStatement) ? ')' : '',
    optionsStatement,
    hasContent(columnsStatement) ? ';' : '',
  ]);
};

module.exports = translate;
