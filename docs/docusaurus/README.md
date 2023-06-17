---
title: Documentation Site
slug: /readme
---

This documentation site is built using [Docusaurus 2](https://v2.docusaurus.io/), a modern static website generator.

## System Requirements

https://docusaurus.io/docs/installation#requirements

## Installation

Follow the [CONTRIBUTING_CODE](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md) guide in the `great_expectations` repository to install dev dependencies.

Then run the following command from the repository root to install the rest of the dependencies and build documentation locally (including prior versions) and start a development server:
```console
invoke docs
```

Once you've run `invoke docs` once, you can run `invoke docs --start` to start the development server without copying and building prior versions. Note that if prior versions change your build won't include those changes until you run `invoke docs --clean` and `invoke docs` again.

```console
invoke docs --start
```

To remove versioned code, docs and sidebars run:

```console
invoke docs --clean
```


## Linting

[standard.js](https://standardjs.com/) is used to lint the project. Please run the linter before committing changes.

```console
invoke docs --lint
```

## Build

To build a static version of the site, this command generates static content into the `build` directory. This can be served using any static hosting service.

```console
invoke docs --build
```

## Deployment

Deployment is handled via [Netlify](https://app.netlify.com/sites/niobium-lead-7998/overview).

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

(Note: yarn commands should be run from docs/docusaurus/)

1. It may help to start with a fresh virtualenv and clone of gx.
2. Check out the version from the tag, e.g. `git checkout 0.15.50`
3. Make sure dev dependencies are installed `pip install -c constraints-dev.txt -e ".[test]"`
4. Install API docs dependencies `pip install -r docs/sphinx_api_docs_source/requirements-dev-api-docs.txt`
5. Build API docs `invoke api-docs` from the repo root.
6. Run `yarn install` from `docs/docusaurus/`.
7. Temporarily change onBrokenLinks: 'throw' to onBrokenLinks: 'warn' in `docusaurus.config.js` to allow the build to complete even if there are broken links.
8. Run `yarn build` from `docs/docusaurus/`.
9. Create the version e.g. `yarn docusaurus docs:version 0.15.50` from `docs/docusaurus/`.
10. Pull down the version file (see `docs/build_docs` for the file, currently https://superconductive-public.s3.us-east-2.amazonaws.com/oss_docs_versions.zip)
11. Unzip and add your newly created versioned docs via the following:
12. Copy the version you built in step 4 from inside `versioned_docs` in your repo to the `versioned_docs` from the unzipped version file.
13. Copy the version you built in step 4 from inside `versioned_sidebars` in your repo to the `versioned_sidebars` from the unzipped version file.
14. Add your version number to `versions.json` in the unzipped version file.
15. Zip up `versioned_docs`, `versioned_sidebars` and `versions.json` and upload to the s3 bucket (see `docs/build_docs` for the bucket name)
16. Once the docs are built again, this zip file will be used for the prior versions.
