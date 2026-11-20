const path = require('path');

module.exports = {
  plugins: [
    require('@tailwindcss/postcss')({ base: path.resolve(__dirname) }),
    require('./scripts/scope-styles.cjs')(),
  ],
};
