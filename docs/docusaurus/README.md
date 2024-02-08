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

Once you've run `invoke docs` once, you can run `invoke docs --start` to start the development server without copying and building prior versions.

```console
invoke docs --start
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

## Other relevant files & directories

The following are a few details about other files Docusaurus uses that you may wish to be familiar with.

- `sidebars.js`: JavaScript that specifies the sidebar/navigation used in docs pages
- `static`: static assets used in docs pages (such as CSS) live here
- `docusaurus.config.js`: the configuration file for Docusaurus
- `babel.config.js`: Babel config file used when building
- `package.json`: dependencies and scripts
- `yarn.lock`: dependency lock file that ensures reproducibility
- `src/`: global components live here
- `docs/`: Current version of docs
- `versioned_docs/`: Older versions of docs live here. These are copies of `docs/` from the moment when `docs invoke --version=<VERSION>` was run.
- `versioned_sidebars/`: Older versions of sidebars live here. Similar to `versioned_docs/`

sitemap.xml is not in the repo since it is built and uploaded by a netlify plugin during the documentation build process. 

## Documentation changes checklist

1. For any pages you have moved or removed, update _redirects to point from the old to the new content location


## Versioning

To add a new version, follow these steps:

1. It may help to start with a fresh virtualenv and clone of gx.
1. Make sure dev dependencies are installed `pip install -c constraints-dev.txt -e ".[test]"` and `pip install pyspark`
1. Install API docs dependencies `pip install -r docs/sphinx_api_docs_source/requirements-dev-api-docs.txt`
1. Run `invoke docs version=<MAJOR.MINOR>` (substituting your new version numbers)
1. This will create a new entry in `v


## Versioning and docs build flow
### Versioning
```mermaid
sequenceDiagram
    Participant Code
    Participant SphinxBuild as temp_sphinx_api_docs_build_dir/
    Participant Docusaurus as docs/docusaurus
    Participant DocsBuild as docs/docusaurus/build
    Participant Github
    Participant S3
    Participant Netlify

    loop versioning
        % invoke api-docs
        Code ->> SphinxBuild: sphinx generated html
        activate SphinxBuild
        SphinxBuild ->> Docusaurus: html converted to .md and store in docs/docusaurus/docs/reference/api
        deactivate SphinxBuild

        Code ->> Docusaurus: yarn docusaurus docs:version
    end

    loop invoke docs --build
        % invoke api-docs
        Code ->> SphinxBuild: sphinx generated html
        activate SphinxBuild
        SphinxBuild ->> Docusaurus: html converted to .md and store in docs/docusaurus/docs/reference/api
        deactivate SphinxBuild

        % yarn docusaurus build
        activate DocsBuild
        Docusaurus ->> DocsBuild: build docs and versioned_*
        DocsBuild ->> Netlify: Deploy
        deactivate DocsBuild
    end
