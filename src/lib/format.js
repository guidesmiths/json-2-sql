const moment = require('moment');
const pgFormat = require('pg-format');
const R = require('ramda');
const { readFileSync } = require('fs');
const { dirname } = require('path');

const demomentify = (obj) => (obj instanceof moment ? obj.toDate() : obj);

const escapeEblah = R.replace(/^E'/i, '\'');
const escapeNull = R.replace(/\0/g, '');
const cleanEscaped = R.pipe(escapeEblah, escapeNull);

const extractParams = R.pipe(
  R.match(/%(.+?:([a-zA-Z0-9_]+)?)/g), // extract the named params => ['%I:foo', '%s:bar', '%L:baz']
  R.map(R.split(':'))
);

const processFileIncludes = (sql, basedir) => {
  const fileParams = R.pipe(
    extractParams,  //  => [['%I', 'foo'], ['%s', 'bar'], ['%F', 'foobar']]
    R.filter(R.pipe(R.nth(0), R.equals('%F'))), // => [['%F','foobar']]
    R.map(R.nth(1)) // => ['foobar']
  )(sql);
  return R.reduce((processedSql, fp) => R.replace(`%F:${fp}`, loadFile(basedir, fp), processedSql), sql, fileParams);
};

const processNamedParams = (rawSql, params) => {
  const sql = processFileIncludes(rawSql);
  const lookupParam = R.flip(R.prop)(params);
  const namedParams = R.pipe(
      extractParams,  //  => [['%I', 'foo'], ['%s', 'bar'], ['%L', 'baz']]
      R.transpose,    // group types and names    => [['%I', ''%s', '%L'],['foo', 'bar', 'baz']]
      R.nth(1)        // get the names            => ['foo', 'bar', 'baz']
    )(sql) || [];
  // replace each instance of a named param (':<name>') with nothing, leaving the types in place
  const processedSql = R.reduce((processing, word) => R.replace(R.concat(':', word), '', processing), sql, namedParams);
  // lookup each named parameter's value in the params object
  const subbedParams = R.map(lookupParam, namedParams);
  return [processedSql, subbedParams];
};

const withArray = (sql, args) => cleanEscaped(pgFormat.withArray(sql, R.map(demomentify, args)));

function format(sql, ...args) {
  if (R.is(Object, args[0])) return R.apply(withArray, processNamedParams(sql, args[0]));
  return withArray(sql, args);
}

const formatFile = (sqlFile, namedParams) => {
  const sql = readFileSync(sqlFile, 'utf8');
  const fileSql = processFileIncludes(sql, dirname(sqlFile));
  return format(fileSql, namedParams);
};

module.exports = { formatFile };
