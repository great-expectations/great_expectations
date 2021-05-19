---
title: Documentation Site
slug: /readme
---

This documentation site is built using [Docusaurus 2](https://v2.docusaurus.io/), a modern static website generator.

## System Requirements

https://docusaurus.io/docs/installation#requirements

## Installation

From the `docs` directory run:

```console
yarn install
```

## Local Development

For the fastest iterative dev loop, start a local server and open up the compiled site in a browser window. Most changes are reflected live without needing server restarts.

```console
yarn start
```

## Linting

[standard.js](https://standardjs.com/) is used to lint the project. Please run the linter before committing changes.

```console
yarn lint
```

## Build

To build a static version of the site, this command generates static content into the `build` directory. This can be served using any static hosting service.

```console
yarn build
```

## Deployment

```console
GIT_USER=<Your GitHub username> USE_SSH=true yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.

## Other relevant files

The following are a few details about other files docusaurus uses that you may wish to be familiar with.

* `../sidebars.js`: javascript that specifies the sidebar/navigation used in docs pages
* `../src`: non-docs pages live here
* `../static`: static assets used in docs pages (such as css) live here
* `../docusaurus.config.js`: the configuration file for docusaurus
* `../babel.config.js`: babel config file used when building
* `../package.json`: dependencies and scripts
* `../yarn.lock`: dependency lock file that ensures reproducibility

## Style guide / hints

* Headings in a page must be ## (h2) or below (h3, h4, etc) in order to render correctly in a table of contents.
* Page and directory names should be separated by dashes (-), not underscores (_). E.g. `why-use-ge.md`
* Every page should have a title element
* Admonition boxes: I'm currently only using "info" (bright blue by default) and "caution" (yellow)
* Tabbed content appears to be sensitive to indentation and seems to require 2 spaces
* We should consistently use `*` for unordered lists (based on the old docs which mainly used `*` instead of `-`, and `1.` for ordered lists.
* Everything in the [old style guide](https://docs.greatexpectations.io/en/latest/contributing/style_guide.html) re: formatting, capitalization, etc. still applies
