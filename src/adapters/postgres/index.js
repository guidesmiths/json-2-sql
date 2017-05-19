const operations = require('require-all')(__dirname);

const translate = (json) => {
  const { type } = json;
  if (!type) throw new Error('Your data needs an operation type!!');
  if (!operations[type]) throw new Error(`Your data contains an unknown type ${type}`);
  return operations[type](json);
};

module.exports = { translate };
