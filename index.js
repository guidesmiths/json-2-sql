const { prop, __ } = require('ramda');
const adapters = require('require-all')(`${__dirname}/src/adapters`);
module.exports = prop(__, adapters);
