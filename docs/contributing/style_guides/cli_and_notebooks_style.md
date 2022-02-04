---
title: CLI and Notebook style guide
---

:::info Note
This style guide will be enforced for all incoming PRs. However, certain legacy areas within the repo do not yet fully adhere to the style guide. We welcome PRs to bring these areas up to code.
:::

### The CLI

The [CLI](../../guides/miscellaneous/how_to_use_the_great_expectations_cli.md) has some conventions of its own.

* The CLI never writes to disk without asking first.
* Questions are always phrased as conversational sentences.
* Sections are divided by headers: “========== Profiling ==========”
* We use punctuation: Please finish sentences with periods, questions marks, or an occasional exclamation point.
* Keep indentation and line spacing consistent! (We’re pythonistas, natch.)
* Include exactly one blank line after every question.
* Within those constraints, shorter is better. When in doubt, shorten.
* Clickable links (usually to documentation) are blue.
* Copyable bash commands are green.
* All top-level bash commands must be nouns: “docs build”, not “build docs”
