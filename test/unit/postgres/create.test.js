const R = require('ramda');
const expect = require('expect.js');
const resourcesJson = require('../../test-schemas/resources/resources.json');

const { translate } = require('../../..')('postgres').index; // TODO filter out

const translateCreation = R.pipe(
  R.merge({ type: 'create' }),
  translate
);

describe('Postgres create adapter', () => {

  it('should throw an error if no type is specified', (done) => {
    try {
      const translation = translate({ schema: 'foo', table: 'bar' });
    } catch({ message }) {
      expect(message).to.equal('Your data needs an operation type!!');
      done()
    }
  });

  it('should throw an error if type is unsupported', (done) => {
    try {
      const translation = translate({ type: 'non-existent', schema: 'foo', table: 'bar' });
    } catch({ message }) {
      expect(message).to.equal('Your data contains an unknown type non-existent');
      done()
    }
  });

  it('should escape camelcase schema and table', () => {
    const translation = translateCreation({ schema: 'fOO', table: 'bAR' });
    expect(translation).to.equal('CREATE TABLE IF NOT EXISTS "fOO"."bAR"');
  });

  it('should handle simplest column', () => {
    const translation = translateCreation({ columns: [ { name: 'id', type: 'INT' } ] });
    expect(translation).to.match(/id INT/);
  });

  it('should join columns', () => {
    const translation = translateCreation({
      columns: [
        { name: 'id', type: 'INT' },
        { name: 'price', type: 'FLOAT4'},
      ]
    });
    expect(translation).to.match(/id INT,\nprice FLOAT4/);
  });

  it('should generate primary key', () => {
    const translation = translateCreation({ primaryKey: 'foo' });
    expect(translation).to.match(/,PRIMARY KEY\(foo\)/);
  });

  it('should escape camelcase primary key', () => {
    const translation = translateCreation({ primaryKey: 'fOO' });
    expect(translation).to.match(/,PRIMARY KEY\("fOO"\)/);
  });

  it('should generate compound primary key', () => {
    const translation = translateCreation({ primaryKey: ['foo', 'bar', 'baz'] });
    expect(translation).to.match(/,PRIMARY KEY\(foo,bar,baz\)/);
  });

  it('should generate index', () => {
    const translation = translateCreation({ schema: 'foo', table: 'bar', indexes: [ ['id'] ] });
    expect(translation).to.equal(`CREATE TABLE IF NOT EXISTS foo.bar
do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = 'foo'
        and tablename = 'bar'
        and indexname = 'bar_id_idx'
)
then
    create index bar_id_idx ON foo.bar(id);
end if;
end
$$;
`);
  });

  it('escapes camelcase index', () => {
    const translation = translateCreation({ schema: 'fOO', table: 'bAR', indexes: [ ['bAZ'] ] });
    expect(translation).to.equal(`CREATE TABLE IF NOT EXISTS "fOO"."bAR"
do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = 'fOO'
        and tablename = 'bAR'
        and indexname = 'bAR_bAZ_idx'
)
then
    create index "bAR_bAZ_idx" ON "fOO"."bAR"("bAZ");
end if;
end
$$;
`);
  });

  it('generates compound index', () => {
    const translation = translateCreation({ schema: 'foo', table: 'bar', indexes: [ ['id', 'name'] ] });
    expect(translation).to.equal(`CREATE TABLE IF NOT EXISTS foo.bar
do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = 'foo'
        and tablename = 'bar'
        and indexname = 'bar_id_name_idx'
)
then
    create index bar_id_name_idx ON foo.bar(id,name);
end if;
end
$$;
`);
  });

  it('generates multiple indexes', () => {
    const translation = translateCreation({ schema: 'foo', table: 'bar', indexes: [ ['id'], ['name'] ] });
    expect(translation).to.equal(`CREATE TABLE IF NOT EXISTS foo.bar
do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = 'foo'
        and tablename = 'bar'
        and indexname = 'bar_id_idx'
)
then
    create index bar_id_idx ON foo.bar(id);
end if;
end
$$;

do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = 'foo'
        and tablename = 'bar'
        and indexname = 'bar_name_idx'
)
then
    create index bar_name_idx ON foo.bar(name);
end if;
end
$$;
`);
  });

  it('puts it all together', () => {
    const translation = translateCreation({
      schema: 'music',
      table: 'albums',
      description: 'This is table of musical albums',
      columns: [
        {name: 'id', type: 'INT', nullable: false, encode: 'DELTA32K'},
        {name: 'title', type: 'VARCHAR', length: 256, encode: 'ZSTD'},
        {name: 'dateReleased', type: 'DATE', encode: 'LZO'},
      ],
      primaryKey: 'id',
      indexes: [
        ['dateReleased'],
      ],
      distStyle: 'KEY',
      distKey: 'title',
      sortKey: 'id',
    }
  );

    expect(translation).to.equal(`CREATE TABLE IF NOT EXISTS music.albums
(
id INT NOT NULL,
title VARCHAR(256),
"dateReleased" DATE
,PRIMARY KEY(id)
)
;
do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = 'music'
        and tablename = 'albums'
        and indexname = 'albums_dateReleased_idx'
)
then
    create index "albums_dateReleased_idx" ON music.albums("dateReleased");
end if;
end
$$;
`);
  });

  it('should create table', () => {
    const translation = translateCreation({ schema: 'foo', table: 'bar' });
    expect(translation).to.equal('CREATE TABLE IF NOT EXISTS foo.bar');
  });

  it('should put it all together', () => {
    const translation = translateCreation({
      schema: 'music',
      table: 'albums',
      description: 'This is table of musical albums',
      columns: [
        {name: 'id', type: 'INT', nullable: false, encode: 'DELTA32K'},
        {name: 'title', type: 'VARCHAR', length: 256, encode: 'ZSTD'},
        {name: 'dateReleased', type: 'DATE', encode: 'LZO'},
      ],
      primaryKey: 'id',
      indexes: [
        ['dateReleased'],
      ],
      distStyle: 'KEY',
      distKey: 'title',
      sortKey: 'id',
    }
  );
    expect(translation).to.equal(`CREATE TABLE IF NOT EXISTS music.albums
(
id INT NOT NULL,
title VARCHAR(256),
"dateReleased" DATE
,PRIMARY KEY(id)
)
;
do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = 'music'
        and tablename = 'albums'
        and indexname = 'albums_dateReleased_idx'
)
then
    create index "albums_dateReleased_idx" ON music.albums("dateReleased");
end if;
end
$$;
`);
  });

  it('should generate complex table', () => {
    const translation = translate(resourcesJson);
    const expected = `CREATE TABLE IF NOT EXISTS resources.resources
(
id INT NOT NULL,
"authorId" BIGINT,
title VARCHAR(1000),
description VARCHAR(65535),
url VARCHAR(300),
private BOOLEAN,
"allowComments" BOOLEAN,
featured BOOLEAN,
"createdDate" TIMESTAMPTZ,
"modifiedDate" TIMESTAMPTZ,
"firstPublishedDate" TIMESTAMPTZ,
"recommendedDate" TIMESTAMPTZ,
deleted BOOLEAN,
draft BOOLEAN,
createdtime TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'utc'),
licence VARCHAR(30),
priceband INT,
country CHAR(2),
"smlResourceId" INT,
bundle BOOLEAN DEFAULT FALSE,
price INT
,PRIMARY KEY(id)
)
;`;

    expect(translation).to.equal(expected);
  });
});
