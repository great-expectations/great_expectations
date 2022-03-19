---
title: How to write a how-to-guide
---

This guide shows how to create a new how-to guide in Great Expectations. By writing guides with consistent structure and styling, you can get your PRs approved faster and make the Great Expectations docs more discoverable, useful, and maintainable.

## Purpose of a how-to guide

- The purpose of a how-to guide is to replicate, **NOT** to teach or explain. Teaching and explaining Great Expectations concepts are covered in the **Core Concepts** reference section.

- Assume that the user has a working deployment of Great Expectations, whether it is set up on the file system by running `great_expectations init` or configured in-memory.

- Assume that the user is already familiar with core concepts in Great Expectations: Expectations, Data Contexts, Validation, Datasources, etc. etc. You don’t need to spend any time explaining these things.

- If you’re integrating with another system, assume that the user is familiar with that system. ie. If you’re writing the "How to configure a Snowflake Datasource," don’t spend any words explaining Snowflake or any of its core concepts.

- If there are important, non-obvious principles for how Great Expectations relates to other systems, you may include them in the guide. If they are short (1-2 sentences, max), they can go in the Steps section. Otherwise, please put them in Additional Notes.

- Remember, the goal is to help users successfully replicate specific steps as simply as possible. Surprisingly often, it turns out to best to not include explanation at all, since it can distract from the main purpose of the guide. If you feel you must include it, shorter is better.

# Docusaurus background

Documentation for Great Expectations is built using Docusaurus 2, a modern static website generator.  This allows for documentation to be written and built quickly, and for the code that is displayed in the how-to-guides to be tested.  This means that whenever a code-snippet or configuration is displayed in the how-to-guide, it is actually part of a script that is fully integration tested.

## Why test documentation?

Testing provides guarantees that code or configuration displayed in the how-to-doc is fully integration tested with the most-current version of Great Expectations.  Since Great Expectations is a rapidly-changing codebase, there is a great deal of value making sure that the documentation is always up-to-date with the most current changes. Because the script is so central to writing the guides, we highly recommend that you **write the script first, before writing the how-to-guide**.

## What does it mean for contributors?

Typically, a PR for a how-to-guide will contain the following:

1. The script(s) and integration test.
    - The script will be located with the other integration tests in the `great-expectations/tests/integration/docusaurus` directory. **Please keep these directory structures the same as the markdown structures for sanity.**
    - The script will be added to integration tests in the `great-expectation/tests/integration/test_script_runner.py` file

2. The Markdown file in the `great-expectations/docs` folder.

# How to write a how-to-guide script

An example script is found in `/tests/integration/docusaurus/template/script_example.py`.

```python file=../../../tests/integration/docusaurus/template/script_example.py#L1-L9
```

It does the following :

1. Loads required libraries.
2. Loads a Data Context from the `great_expectations/` folder.
3. Makes an assertion about the Data Context.

Your script will likely do more, but this is a great starting point! :)

# How to run the how-to-guide script

In order to run your how-to-guide script, you will to modify the file at `great-expectation/tests/integration/test_script_runner.py`, which runs the integration tests for documentation.

Each integration test is defined by a dictionary entry similar to the one below:

```python
{
    "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py",
    "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    "data_dir": "tests/test_sets/taxi_yellow_tripdata_samples",
},
```

- The path to the `user_flow_script` is the path of the script we are adding; `pandas_python_example.py` in our case.
- `data_context_dir` is a fixture that will be copied into a directory where your script will be run. It will ensure that you can load a `DataContext`, although no additional features will be included.
- `data_dir` is an optional parameter for data files that you can use in your tests. The example contains the path to the NYC yellow taxi trip data, which is a fixture that is used in many tests for Great Expectations.

Once the appropriate steps have been added, the tests can be run using the following pytest command:

`pytest -v --docs-tests -m docs tests/integration/test_script_runner.py`

# How to write a how-to-guide

Begin writing the how-to-guide around the script you wrote in the previous step. The structure of the guide will generally be:

1. Title

2. Purpose

3. Steps

4. Additional Notes (optional)

5. Next Steps (optional)

**Title**: “How to X” See the Style guide for specific guidance on how to phrase and format titles.

**Purpose paragraph**: A single, short paragraph to state the purpose of the guide, and motivate it if necessary.

- “This guide will help you publish a Data Docs site directly to S3. Publishing a site this way makes reviewing and acting on Validation Results easy in a team, and provides a central location to review Expectations.”

Sometimes motivation can be a simple statement of purpose:

- “This guide will help you connect to a MongoDB Datasource.”

If the user has data in Mongo and wants to configure a Datasource, no additional justification is needed.

**Steps**: Steps describe the golden path steps for successful replication.

- Most steps will include inline code, such as a bash command, or an example yml snippet or two. These can be referenced in the how-to-guide itself using relative imports.

- The following snippet displays line 1- line 7 of the `script_example.py` file.  

```markdown
  ```python file=../../../tests/integration/docusaurus/template/script_example.py#L1-L7```
```

:::warning
Make sure that you lint your script before you finalize the line numbers in your Markdown file. This will prevent unintended line changes and save you pain when the linter changes your Python file without you realizing it.

:::
- Most steps will also require user input, such as a connection string that needs to be replaced, or a step that allows for testing (such as running `test_yaml_config()`).

- Snippets should be as short as possible, but no shorter. In general, you can think of the snippet like a diff: what needs to change to accomplish this step?

- Steps should be linear. “Do A, then B, then C.” Avoid complex loops and/or branching. If loops or branching are needed, it is likely a sign that the scope of the guide is too big. In that case, consider options for splitting it into more than one how-to guide.

**Additional notes**: This section covers errata that would be distracting to include in Steps. It’s fine for it to be empty.

**Additional resources**: Additional resources, usually external (i.e. not within the Great Expectations documentation) and usually shown as a list. To avoid link rot, please use this section sparingly, and prefer links to stable, well-maintained resources.

Once a draft of your guide is written, you can see it rendered on a `localhost` by adding your document to the table-of-contents. In Docusaurus this is done by the [sidebar.js file](https://docusaurus.io/docs/sidebar).


## Steps

1. Ensure that your dev environment is set up according to the guide on [Setting up your dev environment](../../contributing/contributing_setup.md)

2. Copy the How-to guide template file to the appropriate subdirectory of `docs/guides/`, and rename it.

3. Write a title and purpose paragraph.

4. Decide whether you’re writing a code-heavy or process-heavy guide, and adjust your formatting appropriately.

5. Fill out the Prerequisites info box (see [How-to guide template file](../../guides/miscellaneous/how_to_template.md)). The header of the info box says: “This how-to guide assumes you have already:”. Place each prerequisite under its own bullet and phrase it using the style in the template: “done something” (e.g. "Configured a Datasource").

6. Fill in the Steps section, making sure to include bash, yml, and code snippets as appropriate.
    - These will typically be included in a separate file that is in the `great_expectations/test/integrations` folder and can be referenced in the how-to-doc. For additional details, please see the "Structure of a How-to-guide" section below.  

7. If needed, add content to Additional Notes and/or Additional Resources. These sections supplement the article with information that would be distracting to include in Steps. It’s fine for them to be empty.

8. Scan your article to make sure it follows the [Style guide](../../contributing/style_guides/docs_style.md). If you’re not familiar with the Style Guide, that’s okay: your PR reviewer will also check for style and let you know if we find any issues.

9. Locally run integration tests for any code that was included as part of the guide. Also see our guide on [Testing](../../contributing/contributing_test.md)

10. Submit your PR! If there are any additional integrations that need to be run, then please add this to your PR message.

## Code-heavy vs process-heavy guides

Broadly speaking, there are two kinds of How-to Guides: code-heavy and process-heavy. All guides are about following a specific sequence of steps. In code-heavy guides, most or all of the steps are expressed in technical syntax: code snippets, JSON or YAML objects, CLI commands, etc. In process-heavy guides, many of the steps are things that must be done manually.

Most guides are code-heavy. When writing a guide that could go either way, please prefer code-heavy, since they tend to make for better replication. (This guide happens to be process-heavy, because it’s about writing.)

#### Indentation, bolding, and code blocks For code-heavy guides

* Treat the first sentence of each step like a header.

    * Use short, complete, imperative sentences: (“Paste the YAML snippet into your config file”, “Run great_expectations init”)

    * Header text should be bold.


* Indent content within steps.

* Any time the user needs to do something, it should be in a code block.

    * Please follow this convention even if the text in the code block is somewhat redundant against the text of the step.

    * Clear, sequential code blocks are easy for the eye to follow. They encourage a health copy-and-modify development pattern.

    * All of these styles are modeled in the How-to guide template file. If you use that template as your guide, you’ll be off to a very good start.

#### For process-heavy guides

* Do not separate headers or bold first sentences.

* Avoid big blocks of text without visual cues for how to read it. Indentation and sub-bullets are your friends.

* When including a code block, please follow the same conventions as for code-heavy guides.

####  Using tabs to differentiate guides for different APIs.

During the process of writing documentation for Great Expectations 0.13, there arose a need to differentiate between documentation for GE up to 0.12.x, and GE 0.13 and beyond.

The use of content-tabs allows for both documentation to co-exist in the same how-to-doc.

The following code snippet shows how two tabs (Great and Expectations) can be created with the associated title and content. For more information on content-tabs, please refer to the following link : https://docusaurus.io/docs/next/markdown-features/tabs

```markdown
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

<Tabs
  defaultValue="Great"
  values={[
    {label: 'Great', value: 'great'},
    {label: 'Expectations', value: 'expectations'},
  ]}>
  <TabItem value="great">This is Great!</TabItem>
  <TabItem value="expectations">These are Expectations</TabItem>
</Tabs>
```
