module.exports = (api) => {
  const isTest = api.env("test");
  return {
    presets: [
      ...(!isTest
        ? [require.resolve("@docusaurus/core/lib/babel/preset")]
        : [
            [
              "@babel/preset-env",
              {
                targets: { node: "current" },
              },
            ],
            "@babel/preset-react",
          ]),
    ],
  };
};
