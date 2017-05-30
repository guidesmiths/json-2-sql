const R = require('ramda');
const { ident } = require('pg-format');
const { formatFile } = require('../lib/format');
const { join } = require('path');
const createIndexFile = join(__dirname, '..', 'sql/create-index.sql');

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

const generateColumn = ({ name, type, length, nullable, default: defaultValue, encode }) => {
  const colType = TYPE_MAP[type.toUpperCase()];
  const len = length ? `(${length})` : '';
  const notNull = nullable ? '' : ' NOT NULL';
  const def = defaultValue !== undefined ? ` DEFAULT ${defaultValue}` : '';
  const encodeValue = encode ? ` ENCODE ${encode}` : '';
  return `${ident(name)} ${colType}${len}${notNull}${def}${encodeValue}`;
}

const generateColumns = ({ columns }) => R.map(generateColumn, columns);

const generatePrimaryKey = ({ primaryKey }) => primaryKey ? `,PRIMARY KEY(${ident(primaryKey)})` : '';

const generateIndex = R.curry((schema, table, columns) => {
  const indexName = R.join('_', R.flatten([ table, columns, 'idx']));
  return formatFile(createIndexFile, { schema, table, indexName, columns });
});

const generateTableOptions = ({ distStyle, distKey, sortKey }) => {
  const polish = R.pipe(
    R.filter(R.identity),
    R.join('\n')
  );
  return polish([
    distStyle ? `DISTSTYLE ${distStyle}` : '',
    distKey ? `DISTKEY(${distKey})` : '',
    sortKey ? `SORTKEY(${sortKey})` : '',
  ]);
};

const generateIndexes = ({ schema, table, indexes }) => R.map(generateIndex(schema, table), indexes);


module.exports = {
  DEFAULT_TABLE,
  DEFAULT_COLUMN,
  generateCreate,
  generatePrimaryKey,
  generateTableOptions,
  generateIndexes,
  generateColumns
};
