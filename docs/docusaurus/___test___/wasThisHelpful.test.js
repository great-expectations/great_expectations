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
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));

        expect(screen.getByText('Yes')).toBeDisabled();
        expect(screen.getByText('No')).toBeDisabled();
    });

    test("Feedback Modal should pop-up when 'No' button has been clicked", async () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));

        expect(screen.getByTestId('feedback-modal')).toBeInTheDocument();
    });

    test("Submit button in Feedback Modal is disabled when description is blank", async () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));

        expect(screen.getByText("Submit")).toBeDisabled();
    });

    test("Submit button in Feedback Modal is enabled when description input is filled", async () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));

        const descriptionBox = screen.getByTestId('description-textbox');

        await userEvent.type(descriptionBox,'I have a problem');

        expect(screen.getByText("Submit")).not.toBeDisabled();
    });

    test("", async () => {
        render(
            <WasThisHelpful/>
        );
        await userEvent.click(screen.getByText('No'));

        const descriptionBox = screen.getByTestId('description-textbox');

        await userEvent.type(descriptionBox,'I have a problem');

        expect(screen.getByText("Submit")).not.toBeDisabled();
    });

});