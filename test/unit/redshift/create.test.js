const R = require('ramda');
const expect = require('expect.js');
const resourcesJson = require('../../test-schemas/resources/resources.json');

const { redshift: translate } = require('../../..');

const translateCreation = R.pipe(
  R.merge({ type: 'create' }),
  translate
);

describe('Redshift create adapter', () => {

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

  it('should passthru column types it doesnt recognize', () => {
    const translation = translateCreation({ columns: [ { name: 'id', type: 'TIMESTAMP WITHOUT TIME ZONE' } ] });
    expect(translation).to.match(/id TIMESTAMP WITHOUT TIME ZONE/);
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

  it('adds column encodings', () => {
    const translation = translateCreation({
      columns: [{ name: 'id', type: 'INT', encode: 'ZSTD' }]
    });
    expect(translation).to.match(/id INT ENCODE ZSTD/);
  });

  it('ignores indexes', () => {
    const translation = translateCreation({
      indexes: [ ['id'] ]
    });
    expect(translation).to.not.match(/indexname/);
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
id INT NOT NULL ENCODE DELTA32K,
title VARCHAR(256) ENCODE ZSTD,
"dateReleased" DATE ENCODE LZO
,PRIMARY KEY(id)
)
DISTSTYLE KEY
DISTKEY(title)
SORTKEY(id)
;`);
  });

  it('generates complex table', () => {
    const translation = translateCreation(resourcesJson);
    const expected = `CREATE TABLE IF NOT EXISTS resources.resources
(
id INT NOT NULL ENCODE DELTA32K,
"authorId" BIGINT ENCODE LZO,
title VARCHAR(1000) ENCODE LZO,
description VARCHAR(65535) ENCODE LZO,
url VARCHAR(300) ENCODE LZO,
private BOOLEAN,
"allowComments" BOOLEAN,
featured BOOLEAN,
"createdDate" TIMESTAMPTZ ENCODE LZO,
"modifiedDate" TIMESTAMPTZ ENCODE LZO,
"firstPublishedDate" TIMESTAMPTZ ENCODE LZO,
"recommendedDate" TIMESTAMPTZ ENCODE LZO,
deleted BOOLEAN,
draft BOOLEAN,
createdtime TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'utc') ENCODE LZO,
licence VARCHAR(30) ENCODE LZO,
priceband INT ENCODE LZO,
country CHAR(2) ENCODE LZO,
"smlResourceId" INT ENCODE LZO,
bundle BOOLEAN DEFAULT FALSE,
price INT ENCODE LZO
,PRIMARY KEY(id)
)
DISTSTYLE ALL
SORTKEY(id)
;`;

    expect(translation).to.equal(expected);
  });
});
