---
title: How to use a Custom Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Use the information provided here to use Custom Expectations you created or imported from the Great Expectations Experimental Library.

Custom <TechnicalTag tag="expectation" text="Expectations"/> extend the core functionality of Great Expectations for a specific purpose or business need. Often, they are less stable and less mature than the core library. For these reasons they are not available from the core library, and they must be registered and imported when you create an <TechnicalTag tag="expectation_suite" text="Expectation Suite"/>, and when you define and run a <TechnicalTag tag="checkpoint" text="Checkpoint"/>.

When you instantiate your <TechnicalTag tag="data_context" text="Data Context"/>, all plugins in the `great_expectations/plugins` directory are automatically available,
and this allows you to import your Custom Expectation from other locations.

## Prerequisites

<Prerequisites>

- A <TechnicalTag tag="custom_expectation" text="Custom Expectation"/> or a Custom Expectation from the [Great Expectations Experimental Library](https://github.com/great-expectations/great_expectations/tree/develop/contrib/experimental/great_expectations_experimental/expectations)

</Prerequisites>

## Import a custom Expectation you created

1. Add your Custom Expectation to the `great_expectations/plugins/expectations` folder of your Great Expectations deployment.

2. Run a command similar to the following:

<!--A snippet is required for this code block.-->

    ```python
    from expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical
    # ...
    validator.expect_column_values_to_be_alphabetical(column="test")
    ```

## Import a contributed custom Expectation

If you're using a Custom Expectation from the `Great Expectations Experimental` library, you'll need to import it. 

1. Run `pip install great_expectations_experimental`.

2. Run a command similar to the following:

<!--A snippet is required for this code block.-->

    ```python
    from great_expectations_experimental.expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical
    # ...
    validator.expect_column_values_to_be_alphabetical(column="test")
    ```











