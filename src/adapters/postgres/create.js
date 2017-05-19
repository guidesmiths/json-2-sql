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

const normaliseTable = R.merge(DEFAULT_TABLE);
const normaliseColumns = R.map(R.merge(DEFAULT_COLUMN));

const translate = (table) => {

  const normalised = R.merge(DEFAULT_TABLE, table);
  const normalisedColumns = R.merge(normalised, { columns: R.map(R.merge(DEFAULT_COLUMN), normalised.columns) });

  const createStatement = generateCreate(normalisedColumns);
  const columnsStatement = generateColumns(normalisedColumns);
  const pKeyStatement = generatePrimaryKey(normalisedColumns);
  const indexesStatement = generateIndexes(normalisedColumns);

  const hasContent = R.complement(R.isEmpty);

  const bits = [
    ...createStatement,
    hasContent(columnsStatement) ? '(' : '',
    columnsStatement.join(',\n'),
    pKeyStatement,
    hasContent(columnsStatement) ? ')' : '',
    hasContent(columnsStatement) ? ';' : '',
    ...indexesStatement,
  ];

  return bits.filter(Boolean).join('\n');
};

module.exports = translate;
