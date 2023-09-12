import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

There are many Expectations available for you to use.  To demonstrate the creation of an Expectation through the use of the Validator you defined earlier, here are examples of the process for two of them:

```python name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py add_expectations"
```

Each time you evaluate an Expectation with `validator.expect_*`, the Expectation is immediately Validated against your provided Batch of data. This instant feedback helps you identify unexpected data quickly. The Expectation configuration is stored in the Expectation Suite you provided when the Validator was initialized.

You can also create Expectation Suites using a [Data Assistant](../../../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md) to automatically create expectations based on your data or [manually using domain knowledge and without inspecting data directly](../../../guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly.md). 

To find out more about the available Expectations, see the [Expectations Gallery](https://greatexpectations.io/expectations).