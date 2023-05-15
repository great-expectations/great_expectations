---
title: Documentation Site
slug: /readme
---

This documentation site is built using [Docusaurus 2](https://v2.docusaurus.io/), a modern static website generator.

## System Requirements

https://docusaurus.io/docs/installation#requirements

## Installation

From this folder (docs/docusaurus) run:

```console
yarn install
```

## Local Development

For the fastest iterative dev loop, start a local server and open up the compiled site in a browser window. Most changes are reflected live without needing server restarts. Run this and all commands in the `/docs/docusaurus/` folder.

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

- `sidebars.js`: JavaScript that specifies the sidebar/navigation used in docs pages
- `src`: non-docs pages live here
- `static`: static assets used in docs pages (such as CSS) live here
- `docusaurus.config.js`: the configuration file for Docusaurus
- `babel.config.js`: Babel config file used when building
- `package.json`: dependencies and scripts
- `yarn.lock`: dependency lock file that ensures reproducibility

sitemap.xml is not in the repo since it is built and uploaded by a netlify plugin during the documentation build process. 

## Documentation changes checklist

1. For any pages you have moved or removed, update _redirects to point from the old to the new content location


## Versioning

To add a new version, follow these steps:

(Note: yarn commands should be run from the repo root prior to PR 7227, and from docs/docusaurus/ afterward)

1. It may help to start with a fresh virtualenv and clone of gx.
2. Check out the version from the tag, e.g. `git checkout 0.15.50`
3. Make sure dev dependencies are installed `pip install -c constraints-dev.txt -e ".[test]"`
4. Install API docs dependencies `pip install -r docs/sphinx_api_docs_source/requirements-dev-api-docs.txt`
5. Build API docs `invoke api-docs` from the repo root.
6. Run `yarn install` from `docs/docusaurus/`.
7. Run `yarn build` from `docs/docusaurus/`.
8. Create the version e.g. `yarn docusaurus docs:version 0.15.50` from `docs/docusaurus/`.
9. Pull down the version file (see `docs/build_docs` for the file, currently https://superconductive-public.s3.us-east-2.amazonaws.com/oss_docs_versions.zip)
10. Unzip and add your newly created versioned docs via the following:
11. Copy the version you built in step 4 from inside `versioned_docs` in your repo to the `versioned_docs` from the unzipped version file.
12. Copy the version you built in step 4 from inside `versioned_sidebars` in your repo to the `versioned_sidebars` from the unzipped version file.
13. Add your version number to `versions.json` in the unzipped version file.
14. Zip up `versioned_docs`, `versioned_sidebars` and `versions.json` and upload to the s3 bucket (see `docs/build_docs` for the bucket name)
15. Once the docs are built again, this zip file will be used for the versions.
