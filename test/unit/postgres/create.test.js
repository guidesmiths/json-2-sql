const R = require('ramda');
const expect = require('expect.js');
const resourcesJson = require('../../test-schemas/resources/resources.json');

const { translate } = require('../../..')('postgres').index; // TODO filter out

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

  it('creates table', () => {
    const translation = translate({ type: "create", schema: 'foo', table: 'bar' });
    expect(translation).to.equal('CREATE TABLE IF NOT EXISTS foo.bar');
  });

  it('puts it all together', () => {
    const translation = translate({
      type: "create",
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

  it('generates complex table', () => {
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
