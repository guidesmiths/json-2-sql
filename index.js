const R = require('ramda');
const adapters = require('require-all')(`${__dirname}/src/adapters`);

const exposeTranslations = R.pipe(
  R.values,
  R.pluck('index'),
  R.pluck('translate'),
  R.zip(R.keys(adapters)),
  R.fromPairs
);

module.exports = exposeTranslations(adapters);
