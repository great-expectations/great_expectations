import * as React from "react";
import { test } from "@jest/globals";

import { render, screen } from "@testing-library/react";
import userEvent from '@testing-library/user-event'
import '@testing-library/jest-dom';
import WasThisHelpful from "../src/components/WasThisHelpful";

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

    test("Buttons should be disabled when 'Yes' button has been clicked", async () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('Yes'));

        expect(screen.getByText('Yes')).toBeDisabled();
        expect(screen.getByText('No')).toBeDisabled();
    });

    test("Buttons should be disabled when 'No' button has been clicked", async () => {
        await selectingNoInWasThisHelpful();

        expect(screen.getByText('Yes')).toBeDisabled();
        expect(screen.getByText('No')).toBeDisabled();
    });

    test("Feedback Modal should pop-up when 'No' button has been clicked", async () => {
        await selectingNoInWasThisHelpful();

        expect(screen.getByText('Tell us more')).toBeInTheDocument();
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

    test("After clicking the submit button, the Feedback modal disappears", async () => {
        await selectingNoInWasThisHelpful();

        await completeRequiredFields();

        await userEvent.click(screen.getByText("Submit"));

        expect(screen.queryByText('Tell us more')).not.toBeInTheDocument();
    });

    async function selectingNoInWasThisHelpful() {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));
    }

    async function completeRequiredFields() {
        const descriptionBox = screen.getByPlaceholderText('Provide as much detail as possible about the issue you ' +
                                              'experienced or where improvement is needed. Detailed feedback helps ' +
                                                          'us better identify the problem and determine a solution.');

        await userEvent.type(descriptionBox, 'I have a problem');
    }

});