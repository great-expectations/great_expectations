const path = require('path');
const MergeIntoSingleFilePlugin = require('webpack-merge-and-include-globally');

module.exports = {
  entry: './app.js',
  output: {
    filename: 'scripts/app.js',
    path: path.resolve(__dirname, 'static'),
  },
  plugins: [
    new MergeIntoSingleFilePlugin({
      files: {
        'scripts/vendor.js': [
            './node_modules/vega/build/vega.min.js',
            './node_modules/vega-lite/build/vega-lite.min.js',
            './node_modules/vega-embed/build/vega-embed.min.js',
            './node_modules/@fortawesome/fontawesome-free/js/fontawesome.min.js',
            './node_modules/jquery/dist/jquery.min.js',
            './node_modules/popper.js/dist/umd/popper.min.js',
            './node_modules/bootstrap/dist/js/bootstrap.min.js',
            './node_modules/bootstrap-table/dist/bootstrap-table.min.js',
            './node_modules/bootstrap-table/dist/extensions/filter-control/bootstrap-table-filter-control.min.js',
            './node_modules/bootstrap-datepicker/dist/js/bootstrap-datepicker.min.js'
        ],
        'styles/vendor.css': [
             './node_modules/bootstrap-table/dist/bootstrap-table.min.css',
             './node_modules/bootstrap/dist/css/bootstrap.min.css',
             './node_modules/bootstrap-table/dist/extensions/filter-control/bootstrap-table-filter-control.min.css',
             './node_modules/bootstrap-datepicker/dist/css/bootstrap-datepicker.min.css',
             './node_modules/@forevolve/bootstrap-dark/dist/css/bootstrap-prefers-dark.css'
        ]
      }
    })
  ]
};
