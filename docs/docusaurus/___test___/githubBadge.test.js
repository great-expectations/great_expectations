import * as React from "react";
import { jest, test } from "@jest/globals";

import { render, screen } from "@testing-library/react";
import GithubBadge from "../src/components/NavbarItems/GithubNavbarItem";

// Todo: replace this with a mock server if we need to do more fetch requests: https://testing-library.com/docs/react-testing-library/example-intro/#full-example
global.fetch = jest.fn(() =>
  Promise.resolve({
    json: () => Promise.resolve({ stargazers_count: 9100, forks_count: 20000 }),
  })
);

afterEach(() => {
  jest.restoreAllMocks();
});
describe("github badge in the header", () => {
  test("should show stargazers count", async () => {
    render(
      <GithubBadge owner="great-expectations" repository="great_expectations" />
    );
    await screen.findByText("9.1k");
    screen.getByText("20k");
  });
});
