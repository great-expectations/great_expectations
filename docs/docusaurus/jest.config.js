module.exports = {
    collectCoverage: true,
    collectCoverageFrom: ['src/**/*.{js,jsx}'],
    coverageDirectory: 'coverage',
    testEnvironment: 'jsdom',
    moduleNameMapper: {
        "\\.css$": "identity-obj-proxy",
        "@docusaurus/(BrowserOnly|ComponentCreator|constants|ExecutionEnvironment|Head|Interpolate|isInternalUrl|Link|Noop|renderRoutes|router|Translate|use.*)":
            "identity-obj-proxy",
        "@theme/(.*)": "@docusaurus/theme-classic/src/theme/$1",
        "@site/(.*)": "website/$1",
    },
    transform: {
        '^.+\\.(js|jsx)$': ['babel-jest', { configFile: './babel.config.js' }],
    },
    globals: {
        fetch: global.fetch
    }
}
