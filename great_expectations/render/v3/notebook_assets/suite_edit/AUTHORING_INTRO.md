## Create & Edit Expectations

{% if batch_request %}
Add expectations by calling specific expectation methods on the `validator` object. They all begin with `.expect_` which makes autocompleting easy using tab.

Because you selected interactive mode, you are now creating or editing an Expectation Suite with validator feedback from the sample batch of data that you specified (see `batch_request`).

Note that if you select manual mode you may still create or edit an Expectation Suite directly, without feedback from the `validator`. See our documentation for more info and examples: [How to create a new Expectation Suite without a sample batch](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly).

{% else %}
You are adding Expectation configurations to the suite. Since you selected manual mode, there is no sample batch of data and no validation happens during this process. See our documentation for more info and examples: **[How to create a new Expectation Suite without a sample batch](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly)**.

Note that if you do use interactive mode you may specify a sample batch of data to use when creating your Expectation Suite. You can then use a `validator` to get immediate feedback on your Expectations against your specified sample batch.
{% endif %}

You can see all the available expectations in the **[expectation gallery](https://greatexpectations.io/expectations)**.
