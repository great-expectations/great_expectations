import * as React from "react";
import { jest, test } from "@jest/globals";

import { render, screen } from "@testing-library/react";
import DocSidebarWrapper from "../src/theme/DocSidebar/index";

jest.mock('@theme-original/DocSidebar', () => {
    return (props) => {
        return props.path
    }
})

describe("doc sidebar", () => {
    test("should retrieve url with hash if it exists", () => {
        render(
            <DocSidebarWrapper/>
        );
        screen.getByText("www.example.com/section#some-subsection");
    });
});
