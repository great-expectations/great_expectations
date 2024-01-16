---
title: Add comments to Expectations and display them in Data Docs
---
import Prerequisites from '@site/docs/components/_prerequisites.jsx'
import TechnicalTag from '@site/docs/reference/learn/term_tags/_tag.mdx';

This guide will help you add descriptive comments (or notes, here used interchangeably) to <TechnicalTag tag="expectation" text="Expectations" /> and display those comments in <TechnicalTag tag="data_docs" text="Data Docs" />. In these comments you can add some clarification or motivation to the Expectation definition to help you communicate more clearly with your team about specific Expectations. Markdown is supported in these comments.

## Prerequisites

<Prerequisites>

- [A working Great Expectations deployment](/docs/oss/guides/setup/setup_overview)
- [A Data Context](/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context)
- [An Expectations Suite](/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)

</Prerequisites>

## Edit your Expectation Suite

```bash
great_expectations suite edit <your_suite_name>
```

## Add comments to specific Expectations

For each Expectation you wish to add notes to, add a dictionary to the ``meta`` field with the key ``notes`` and your comment as the value. Here is an example.

```python
validator.expect_table_row_count_to_be_between(
  max_value=1000000, min_value=1,
  meta={"notes": "Example notes about this expectation."}
)
```

Leads to the following representation in the <TechnicalTag tag="data_docs" text="Data Docs" /> (For <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> pages, click on the speech bubble to view the comment).

![Expectation with simple comment, no formatting](/docs/oss/images/table_level_no_format.png)

## Add styling to your comments (Optional)

To add styling to your comments, you can add a format tag. Here are a few examples.

A single line of markdown is rendered in red, with any Markdown formatting applied.

```python
validator.expect_column_values_to_not_be_null(
  column="column_name",
  meta={
      "notes": {
          "format": "markdown",
          "content": "Example notes about this expectation. **Markdown** `Supported`."
      }
  }
)
```

![Expectation with a single line of markdown comment is rendered in red with markdown formatting](/docs/oss/images/single_line_markdown_red.png)

Multiple lines can be rendered by using a list for ``content``; these lines are rendered in black text with any Markdown formatting applied.

```python
validator.expect_column_values_to_not_be_null(
  column="column_name",
  meta={
      "notes": {
          "format": "markdown",
          "content": [
              "Example notes about this expectation. **Markdown** `Supported`.",
              "Second example note **with** *Markdown*",
          ]
      }
  }
)
```

![Multiple lines of markdown rendered with formatting](/docs/oss/images/multiple_line_markdown.png)

You can also change the ``format`` to ``string`` and single or multiple lines will be formatted similar to the above, but the Markdown formatting will not be applied.

```python
validator.expect_column_values_to_not_be_null(
  column="column_name",
  meta={
      "notes": {
          "format": "string",
          "content": [
              "Example notes about this expectation. **Markdown** `Not Supported`.",
              "Second example note **without** *Markdown*",
          ]
      }
  }
)
```

![Multiple lines of string rendered without formatting](/docs/oss/images/multiple_line_string.png)



## Review your comments in the Expectation Suite overview of your Data Docs

You can open your Data Docs by using the `.open_data_docs()` method of your Data Context, which should be present in the last cell of the Jupyter Notebook you did your editing in.