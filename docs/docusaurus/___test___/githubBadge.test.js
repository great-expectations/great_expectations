import * as React from 'react';
import {expect, jest, test} from '@jest/globals';

import { waitFor, render, screen, act } from '@testing-library/react';
import GithubBadge from '../src/components/NavbarItems/GithubNavbarItem';

global.fetch = jest.fn(() =>
    Promise.resolve({
        json: () => Promise.resolve( { stargazers_count: 9100, forks_count: 20 }),
    })
);

jest.mock("@docusaurus/useBaseUrl", () => ({
    __esModule: true,
    default: jest.fn((url) => url),
}));

// describe('github badge in the header', () => {
//     beforeEach(() => {

    // });

    afterEach(() => {
        jest.restoreAllMocks();
    });


    test('should show stargazers count', async () => {
        // await Promise.resolve()

        // act(() => {
            render(<GithubBadge owner="great-expectations" repository="great_expectations"/>);
        // })

        // screen.getByAltText("Github Forks Count");
        // await screen.getByText("9.1k");
    });
// });