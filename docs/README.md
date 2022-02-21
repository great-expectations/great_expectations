---
title: Documentation Site
slug: /readme
---

This documentation site is built using [Docusaurus 2](https://v2.docusaurus.io/), a modern static website generator.

## System Requirements

https://docusaurus.io/docs/installation#requirements

## Installation

From the repo root run:

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

The following are a few details about other files Docusaurus uses that you may wish to be familiar with.

- `../sidebars.js`: JavaScript that specifies the sidebar/navigation used in docs pages
- `../src`: non-docs pages live here
- `../static`: static assets used in docs pages (such as CSS) live here
- `../docusaurus.config.js`: the configuration file for Docusaurus
- `../babel.config.js`: Babel config file used when building
- `../package.json`: dependencies and scripts
- `../yarn.lock`: dependency lock file that ensures reproducibility
- `sitemap.xml`: After any changes to sidebars.js are made, use https://www.xml-sitemaps.com/ with the temporary netlify url and then download the resulting sitemap.xml to update the existing sitemap.xml file. Make sure to change the temporary netlify url to the real docs url https://docs.greatexpectations.io/docs/ in the sitemap.xml file.

## Documentation changes checklist

1. If you have made changes to sidebar.js, regenerate and update sitemap.xml per the above instructions
2. For any pages you have moved or removed, update _redirects to point from the old to the new content location