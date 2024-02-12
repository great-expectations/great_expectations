jest.mock("@docusaurus/useBaseUrl", () =>
  jest.fn(() => {
    const originalModule = jest.requireActual("@docusaurus/useBaseUrl");
    return {
      ...originalModule,
      useBaseUrl: (url) => url,
    };
  })
);
