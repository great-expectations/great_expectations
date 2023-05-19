# Workflows

The Great Experience code base has various places where you can contribute code to. This document describes several workflows you might want to run to get started.

First, make sure you have cloned the repository and installed the Python dependencies. Read more on this in [Contribute a code change](CONTRIBUTING_CODE.md).

This code base provides following workflows:

- [Code Linting](#code-linting)
- [Locally deploy docs](#locally-deploy-docs)
- [Verify links in docs](#verify-links-in-docs)
- [Generate Glossary](#generate-glossary)

## Code Linting

Before submitting a pull request, make sure that your code passes the lint check, for that run:

```sh
black .
ruff . --fix
```

## Locally Deploy Docs

You can find more information on developing Great Expectation docs in [/docs/docusaurus/README.md](/docs/docusaurus/README.md). To get a version of the docs deployed locally, run:

```sh { name=docs background=false }
invoke docs
```

The website should be available at:

```sh
open http://localhost:3000/docs
```

## Verify links in docs

We use a link checker tool to verify that links within our docs are valid, you can run it via:

```sh { name=linkcheck }
python3 docs/checks/docs_link_checker.py -p docs -r docs -s docs --skip-external
```

## Generate Glossary

Generates a glossary page in our docs:

```sh { name=glossary }
python3 scripts/build_glossary_page.py
```
