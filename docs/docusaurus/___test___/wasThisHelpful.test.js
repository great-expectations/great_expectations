import * as React from "react";
import { test } from "@jest/globals";

import { render, screen } from "@testing-library/react";
import userEvent from '@testing-library/user-event'
import '@testing-library/jest-dom';
import WasThisHelpful from "../src/components/WasThisHelpful";

// Todo: replace this with a mock server if we need to do more fetch requests: https://testing-library.com/docs/react-testing-library/example-intro/#full-example
global.fetch = jest.fn(() =>
    Promise.resolve({
        ok: true
    })
);

describe('"Was this Helpful?" section', () => {

    const posthog = {
        capture: (eventName,content) => {}
    }
    Object.defineProperty(global.window, "posthog", {
        value: posthog
    })

    test("Buttons should be enabled when they have not been clicked", () => {
        render(
            <WasThisHelpful/>
        );
        expect(screen.getByText('Yes')).not.toBeDisabled();
        expect(screen.getByText('No')).not.toBeDisabled();
    });

    test("Buttons should disappear and Thank You alert displayed when Yes is clicked", async () => {
        render(
            <WasThisHelpful/>
        );

        await userEvent.click(screen.getByText('Yes'));

        expect(screen.queryByText('Yes')).not.toBeInTheDocument();
        expect(screen.queryByText(THANK_YOU_MESSAGE));
    });

    test("Feedback Modal should pop-up when 'No' button has been clicked", async () => {
        await selectingNoInWasThisHelpful();

        expect(screen.queryByText('What is the problem?')).toBeInTheDocument();
    });

    test("Submit button in Feedback Modal is disabled when description is blank", async () => {
        await selectingNoInWasThisHelpful();

        expect(screen.getByText("Submit")).toBeDisabled();
    });

    test("Submit button in Feedback Modal is enabled when required fields are filled", async () => {
        await selectingNoInWasThisHelpful();

        await completeRequiredFields();

        expect(screen.getByText("Submit")).not.toBeDisabled();
    });

    test("After clicking the submit button, the Feedback modal disappears and Thank You alert displayed", async () => {
        await selectingNoInWasThisHelpful();

        await completeRequiredFields();

        await userEvent.click(screen.getByText("Submit"));

        expect(screen.queryByText('What is the problem?')).not.toBeInTheDocument();
        expect(screen.queryByText(THANK_YOU_MESSAGE));
    });

    test("After clicking No, the Thank You alert is not displayed while the modal is open", async () => {
        await selectingNoInWasThisHelpful();

        expect(screen.queryByText('What is the problem?')).toBeInTheDocument();
        expect(screen.queryByText(THANK_YOU_MESSAGE)).not.toBeInTheDocument();
    });

    async function selectingNoInWasThisHelpful() {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));
    }

    async function completeRequiredFields() {
        await userEvent.click(screen.getByLabelText('Language Typo'));

        const descriptionBox = screen.getByPlaceholderText("Describe the typo that you've found.");

        await userEvent.type(descriptionBox, 'I have a problem');
    }

    const THANK_YOU_MESSAGE = "Thank you for helping us improve our documentation!"

});
