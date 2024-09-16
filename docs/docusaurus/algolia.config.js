module.exports = {
    // Algolia DocSearch configuration
    // Refer to Algolia's documentation for specific options
    filters: [
        {
            pattern: '**/0.18/oss/templates/**', // Exclude an entire folder
            excluded: true,
        },
    ],
};
