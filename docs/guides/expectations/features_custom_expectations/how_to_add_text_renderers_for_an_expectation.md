---
title: How to add Statement Renderers for Custom Expectations
---

import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you implement renderers for your custom Expectations, allowing you to control how your custom Expectations are displayed in Data Docs. Implementing renderers as part of your custom Expectations is not strictly required - if not provided, Great Expectations will render your Expectation using a basic default renderer:

![Expectation rendered using default renderer](../../../images/expectation_fallback.png)

<Prerequisites>

 - Created a [Custom Expectation](../creating_custom_expectations/overview.md)

</Prerequisites>

:::warning
See also this [complete custom expectation with renderer example](https://github.com/superconductive/ge_tutorials/tree/main/getting_started_tutorial_final_v3_api/great_expectations/plugins/custom_column_max_example.py).
:::

Renderers allow you to control how your Custom Expectations are displayed in your [Data Docs](../../../reference/data_docs.md).

This guide will walk you through the process of adding Statement Renderers to the Custom Expectation built in the guide for [how to create a Custom Column Aggregate Expectation](../creating_custom_expectations/how_to_create_custom_column_aggregate_expectations.md).

## Steps

### 1. Decide which renderer type to implement

There are three basic types of Statement Renderers:

- `renderer.prescriptive` renders a human-readable form of your Custom Expectation
- `renderer.diagnostic` renders diagnostic information about the results of your Custom Expectation
- `renderer.descriptive` renders 

- **First, decide which renderer types you need to implement.**

  Use the following annotated Validation Result as a guide (most major renderer types represented):

  ![Annotated Validation Result Example](../../../images/validation_result_example.png)

  At minimum, you should implement a renderer with type ``renderer.prescriptive``, which is used to render the human-readable form of your expectation when displaying Expectation Suites and Validation Results. In many cases, this will be the only custom renderer you will have to implement - for the remaining renderer types used on the Validation Results page, Great Expectations provides default renderers that can handle many types of Expectations.

  **Renderer Types Overview**:
    * ``renderer.prescriptive``: renders human-readable form of Expectation from ExpectationConfiguration
    * ``renderer.diagnostic.unexpected_statement``: renders summary statistics of unexpected values if ExpectationValidationResult includes ``unexpected_count`` and ``element_count``
    * ``renderer.diagnostic.unexpected_table``: renders a sample of unexpected values (and in certain cases, counts) in table format, if ExpectationValidationResult includes ``partial_unexpected_list`` or ``partial_unexpected_counts``
    * ``renderer.diagnostic.observed_value``: renders the observed value if included in ExpectationValidationResult

2. **Next, implement a renderer with type ``renderer.prescriptive``.**

  Declare a class method in your custom Expectation class and decorate it with ``@renderer(renderer_type="renderer.prescriptive")``. The method name is arbitrary, but the convention is to camelcase the renderer type and convert the "renderer" prefix to a suffix (e.g. ``_prescriptive_renderer``).  Adding the ``@render_evaluation_parameter_string`` decorator allows [Expectations that use Evaluation Parameters](../../../guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.md) to render the values of the Evaluation Parameters along with the rest of the output.

  The method should have the following signature, which is shared across renderers:

```python
  @classmethod
  @renderer(renderer_type="renderer.prescriptive")
  @render_evaluation_parameter_string
  def _prescriptive_renderer(
      cls,
      configuration: ExpectationConfiguration = None,
      result: ExpectationValidationResult = None,
      language: str = None,
      runtime_configuration: dict = None,
      **kwargs,
  ) -> List[Union[dict, str, RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent,
                  RenderedGraphContent, Any]]:
      assert configuration or result, "Must provide renderers either a configuration or result."
      ...
```

  In general, renderers receive as input either an ExpectationConfiguration (for prescriptive renderers) or an ExpectationValidationResult (for diagnostic renderers) and return a list of rendered elements. The examples below illustrate different ways you might render your expectation - from simple strings to graphs.

<Tabs
  groupId="renderer-types"
  defaultValue='simple_string'
  values={[
  {label: 'Simple String', value:'simple_string'},
  {label: 'String Template', value:'string_template'},
  {label: 'Table', value:'table'},
  ]}>

<TabItem value="simple_string">

**Input:**

```python
example_expectation_config = ExpectationConfiguration(**{
    "expectation_type": "expect_column_value_lengths_to_be_between",
    "kwargs": {
        "column": "SSL",
        "min_value": 1,
        "max_value": 11,
        "result_format": "COMPLETE"
    }
})
```

**Rendered Output:**

![Simple String Example](../../../images/simple_string.png)

**Implementation:**

```python
class ExpectColumnValueLengthsToBeBetween(ColumnMapExpectation):
    ...

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
            cls,
            configuration: ExpectationConfiguration = None,
            result: ExpectationValidationResult = None,
            language: str = None,
            runtime_configuration: dict = None,
            **kwargs,
    ) -> List[Union[dict, str, RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent,
                    RenderedGraphContent, Any]]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        # get params dict with all expected kwargs
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        # build string template
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "values may have any length."
        else:
            at_least_str = (
                "greater than"
                if params.get("strict_min") is True
                else "greater than or equal to"
            )
            at_most_str = (
                "less than" if params.get("strict_max") is True else "less than or equal to"
            )

            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )

                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must be {at_least_str} $min_value and {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["min_value"] is None:
                    template_str = f"values must be {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["max_value"] is None:
                    template_str = f"values must be {at_least_str} $min_value characters long, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must always be {at_least_str} $min_value and {at_most_str} $max_value characters long."

                elif params["min_value"] is None:
                    template_str = f"values must always be {at_most_str} $max_value characters long."

                elif params["max_value"] is None:
                    template_str = f"values must always be {at_least_str} $min_value characters long."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        # return simple string
        return [Template(template_str).substitute(params)]
```
</TabItem>

<TabItem value="string_template">

**Input:**

```python
example_expectation_config = ExpectationConfiguration(**{
    "expectation_type": "expect_column_value_lengths_to_be_between",
    "kwargs": {
        "column": "SSL",
        "min_value": 1,
        "max_value": 11,
        "result_format": "COMPLETE"
    }
})
```

**Rendered Output:**

![String Template Example](../../../images/string_template.png)

**Implementation:**

```python
class ExpectColumnValueLengthsToBeBetween(ColumnMapExpectation):
    ...

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
            cls,
            configuration: ExpectationConfiguration = None,
            result: ExpectationValidationResult = None,
            language: str = None,
            runtime_configuration: dict = None,
            **kwargs,
    ) -> List[Union[dict, str, RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent,
                    RenderedGraphContent, Any]]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        # get params dict with all expected kwargs
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        # build string template
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "values may have any length."
        else:
            at_least_str = (
                "greater than"
                if params.get("strict_min") is True
                else "greater than or equal to"
            )
            at_most_str = (
                "less than" if params.get("strict_max") is True else "less than or equal to"
            )

            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )

                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must be {at_least_str} $min_value and {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["min_value"] is None:
                    template_str = f"values must be {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["max_value"] is None:
                    template_str = f"values must be {at_least_str} $min_value characters long, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must always be {at_least_str} $min_value and {at_most_str} $max_value characters long."

                elif params["min_value"] is None:
                    template_str = f"values must always be {at_most_str} $max_value characters long."

                elif params["max_value"] is None:
                    template_str = f"values must always be {at_least_str} $min_value characters long."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        # return simple string
        return [Template(template_str).substitute(params)]
```
</TabItem>

<TabItem value="table">

:::note 
This example shows how you can render your custom Expectation using different content types.
:::

**Input:**

  ```python
  example_expectation_config = ExpectationConfiguration(**{
      "expectation_type": "expect_column_quantile_values_to_be_between",
      "kwargs": {
          "allow_relative_error": False,
          "column": "OBJECTID",
          "quantile_ranges": {
              "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
              "value_ranges": [
                  [5358, 5360],
                  [26788, 26790],
                  [53576, 53578],
                  [80365, 80367],
                  [101795, 101797]
              ]
          },
          "result_format": "COMPLETE"
      }
  })
  ```

  **Rendered Output:**

  ![Table Example](../../../images/table.png)

  **Implementation:**

  ```python
  class ExpectColumnQuantileValuesToBeBetween(TableExpectation):
      ...

      @classmethod
      @renderer(renderer_type="renderer.prescriptive")
      @render_evaluation_parameter_string
      def _prescriptive_renderer(
          cls,
          configuration=None,
          result=None,
          language=None,
          runtime_configuration=None,
          **kwargs
      ):
          runtime_configuration = runtime_configuration or {}
          include_column_name = runtime_configuration.get("include_column_name", True)
          include_column_name = (
              include_column_name if include_column_name is not None else True
          )
          styling = runtime_configuration.get("styling")
          # get params dict with all expected kwargs
          params = substitute_none_for_missing(
              configuration["kwargs"],
              ["column", "quantile_ranges", "row_condition", "condition_parser"],
          )

          # build string template content
          template_str = "quantiles must be within the following value ranges."

          if include_column_name:
              template_str = "$column " + template_str

          if params["row_condition"] is not None:
              (
                  conditional_template_str,
                  conditional_params,
              ) = parse_row_condition_string_pandas_engine(params["row_condition"])
              template_str = (
                  conditional_template_str
                  + ", then "
                  + template_str[0].lower()
                  + template_str[1:]
              )
              params.update(conditional_params)

          expectation_string_obj = RenderedStringTemplateContent(**{
              "content_block_type": "string_template",
              "string_template": {"template": template_str, "params": params},
          })

          # build table content
          quantiles = params["quantile_ranges"]["quantiles"]
          value_ranges = params["quantile_ranges"]["value_ranges"]

          table_header_row = ["Quantile", "Min Value", "Max Value"]
          table_rows = []

          quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

          for quantile, value_range in zip(quantiles, value_ranges):
              quantile_string = quantile_strings.get(quantile, "{:3.2f}".format(quantile))
              table_rows.append(
                  [
                      quantile_string,
                      str(value_range[0]) if value_range[0] is not None else "Any",
                      str(value_range[1]) if value_range[1] is not None else "Any",
                  ]
              )

          quantile_range_table = RenderedTableContent(**{
              "content_block_type": "table",
              "header_row": table_header_row,
              "table": table_rows,
              "styling": {
                  "body": {
                      "classes": [
                          "table",
                          "table-sm",
                          "table-unbordered",
                          "col-4",
                          "mt-2",
                      ],
                  },
                  "parent": {"styles": {"list-style-type": "none"}},
              },
          })

          # return both string template and table content
          return [expectation_string_obj, quantile_range_table]
  ```
</TabItem>


</Tabs>

3. **If necessary, implement additional renderer types that override the Great Expectations defaults.**

  The default implementations are provided below for reference:

  :::note 
  These renderers do not have to have an output for every Expectation.
  :::

<Tabs
  groupId="additional-renderer-types"
  defaultValue='unexpected_statement'
  values={[
  {label: 'diagnostic.unexpected_statement', value:'unexpected_statement'},
  {label: 'diagnostic.unexpected_table', value:'unexpected_table'},
  {label: 'diagnostic.observed_value', value:'observed_value'},
  ]}>

<TabItem value="unexpected_statement">

  ```python
  @classmethod
  @renderer(renderer_type="renderer.diagnostic.unexpected_statement")
  def _diagnostic_unexpected_statement_renderer(
      cls,
      configuration=None,
      result=None,
      language=None,
      runtime_configuration=None,
      **kwargs,
  ):
      assert result, "Must provide a result object."
      success = result.success
      result_dict = result.result

      if result.exception_info["raised_exception"]:
          exception_message_template_str = (
              "\n\n$expectation_type raised an exception:\n$exception_message"
          )

          exception_message = RenderedStringTemplateContent(
              **{
                  "content_block_type": "string_template",
                  "string_template": {
                      "template": exception_message_template_str,
                      "params": {
                          "expectation_type": result.expectation_config.expectation_type,
                          "exception_message": result.exception_info[
                              "exception_message"
                          ],
                      },
                      "tag": "strong",
                      "styling": {
                          "classes": ["text-danger"],
                          "params": {
                              "exception_message": {"tag": "code"},
                              "expectation_type": {
                                  "classes": ["badge", "badge-danger", "mb-2"]
                              },
                          },
                      },
                  },
              }
          )

          exception_traceback_collapse = CollapseContent(
              **{
                  "collapse_toggle_link": "Show exception traceback...",
                  "collapse": [
                      RenderedStringTemplateContent(
                          **{
                              "content_block_type": "string_template",
                              "string_template": {
                                  "template": result.exception_info[
                                      "exception_traceback"
                                  ],
                                  "tag": "code",
                              },
                          }
                      )
                  ],
              }
          )

          return [exception_message, exception_traceback_collapse]

      if success or not result_dict.get("unexpected_count"):
          return []
      else:
          unexpected_count = num_to_str(
              result_dict["unexpected_count"], use_locale=True, precision=20
          )
          unexpected_percent = (
              num_to_str(result_dict["unexpected_percent"], precision=4) + "%"
          )
          element_count = num_to_str(
              result_dict["element_count"], use_locale=True, precision=20
          )

          template_str = (
              "\n\n$unexpected_count unexpected values found. "
              "$unexpected_percent of $element_count total rows."
          )

          return [
              RenderedStringTemplateContent(
                  **{
                      "content_block_type": "string_template",
                      "string_template": {
                          "template": template_str,
                          "params": {
                              "unexpected_count": unexpected_count,
                              "unexpected_percent": unexpected_percent,
                              "element_count": element_count,
                          },
                          "tag": "strong",
                          "styling": {"classes": ["text-danger"]},
                      },
                  }
              )
          ]
  ```

</TabItem>
<TabItem value="unexpected_table">

  ```python
  @classmethod
  @renderer(renderer_type="renderer.diagnostic.unexpected_table")
  def _diagnostic_unexpected_table_renderer(
      cls,
      configuration=None,
      result=None,
      language=None,
      runtime_configuration=None,
      **kwargs,
  ):
      try:
          result_dict = result.result
      except KeyError:
          return None

      if result_dict is None:
          return None

      if not result_dict.get("partial_unexpected_list") and not result_dict.get(
          "partial_unexpected_counts"
      ):
          return None

      table_rows = []

      if result_dict.get("partial_unexpected_counts"):
          # We will check to see whether we have *all* of the unexpected values
          # accounted for in our count, and include counts if we do. If we do not,
          # we will use this as simply a better (non-repeating) source of
          # "sampled" unexpected values
          total_count = 0
          for unexpected_count_dict in result_dict.get("partial_unexpected_counts"):
              if not isinstance(unexpected_count_dict, dict):
                  # handles case: "partial_exception_counts requires a hashable type"
                  # this case is also now deprecated (because the error is moved to an errors key
                  # the error also *should have* been updated to "partial_unexpected_counts ..." long ago.
                  # NOTE: JPC 20200724 - Consequently, this codepath should be removed by approximately Q1 2021
                  continue
              value = unexpected_count_dict.get("value")
              count = unexpected_count_dict.get("count")
              total_count += count
              if value is not None and value != "":
                  table_rows.append([value, count])
              elif value == "":
                  table_rows.append(["EMPTY", count])
              else:
                  table_rows.append(["null", count])

          # Check to see if we have *all* of the unexpected values accounted for. If so,
          # we show counts. If not, we only show "sampled" unexpected values.
          if total_count == result_dict.get("unexpected_count"):
              header_row = ["Unexpected Value", "Count"]
          else:
              header_row = ["Sampled Unexpected Values"]
              table_rows = [[row[0]] for row in table_rows]
      else:
          header_row = ["Sampled Unexpected Values"]
          sampled_values_set = set()
          for unexpected_value in result_dict.get("partial_unexpected_list"):
              if unexpected_value:
                  string_unexpected_value = str(unexpected_value)
              elif unexpected_value == "":
                  string_unexpected_value = "EMPTY"
              else:
                  string_unexpected_value = "null"
              if string_unexpected_value not in sampled_values_set:
                  table_rows.append([unexpected_value])
                  sampled_values_set.add(string_unexpected_value)

      unexpected_table_content_block = RenderedTableContent(
          **{
              "content_block_type": "table",
              "table": table_rows,
              "header_row": header_row,
              "styling": {
                  "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
              },
          }
      )

      return unexpected_table_content_block
  ```

</TabItem>
<TabItem value="observed_value">

  ```python
  @classmethod
  @renderer(renderer_type="renderer.diagnostic.observed_value")
  def _diagnostic_observed_value_renderer(
      cls,
      configuration=None,
      result=None,
      language=None,
      runtime_configuration=None,
      **kwargs,
  ):
      result_dict = result.result
      if result_dict is None:
          return "--"

      if result_dict.get("observed_value"):
          observed_value = result_dict.get("observed_value")
          if isinstance(observed_value, (int, float)) and not isinstance(
              observed_value, bool
          ):
              return num_to_str(observed_value, precision=10, use_locale=True)
          return str(observed_value)
      elif result_dict.get("unexpected_percent") is not None:
          return (
              num_to_str(result_dict.get("unexpected_percent"), precision=5)
              + "% unexpected"
          )
      else:
          return "--"
```

</TabItem>
</Tabs>

4. **Lastly, test that your renderers are providing the desired output by building your Data Docs site.**

  Use the following CLI command: ``great_expectations docs build``.
