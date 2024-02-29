import * as React from "react";
import { test } from "@jest/globals";

import { render, screen } from "@testing-library/react";
import WasThisHelpful from "../src/components/WasThisHelpful";
import * as userEvent from "react-dom/test-utils";

describe('"Was this Helpful?" section', () => {
    test("Buttons should be enabled when they have not been clicked", () => {
        render(
            <WasThisHelpful/>
        );
        expect(screen.getByText('Yes')).not.toBeDisabled();
        expect(screen.getByText('No')).not.toBeDisabled();
    });

    test("Buttons should be disabled when 'Yes' button has been clicked", () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('Yes'));

        expect(screen.getByText('Yes')).toBeDisabled();
        expect(screen.getByText('No')).toBeDisabled();
    });

    test("Buttons should be disabled when 'No' button has been clicked", () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));

        expect(screen.getByText('Yes')).toBeDisabled();
        expect(screen.getByText('No')).toBeDisabled();
    });

    test("Feedback Modal should pop-up when 'No' button has been clicked", () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));

        expect(screen.getByRole('h5')).toHaveTextContent('Tell us more');
    });
});