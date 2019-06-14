# Introduction to Expectation Renderers

### Why Renderers?

Renderers compile Expectations and Expectation Validation Results (EVRs) to human-readable docs. Compiling allows you to keep your documentation in sync with your tests. As long as you're consistently running pipeline tests, you know that your docs will never go stale.

Great Expectation's renderers are designed to allow for great flexibility and extensibility. We know that there are many ways to describe and visualize data. The Great Expectations approach allows us to encompass widely varying use cases, such as:

- Text summarization to ("74 out of 74 tests passed, with 3 warnings." vs "Detail views of all warnings")
- Rendering to multiple view models: HTML, Slack, etc.
- Rendering within other platforms.
- Rendering to various UI elements, such as tables, bullet lists, graphs, etc.
- Rendering at different levels of data density
- Language localization
- etc.

### Key concepts

**Expectation Validation Results (EVRs)**: a JSON-serializable object generated when attempting to `validate` an Expectation.

**Descriptive** vs **Prescriptive** documentation

- Descriptive docs capture how _this_ data looks _now_. Think profiling, exploration, summarization. Descriptive docs are usually compiled from EVRs, or data samples generated during profiling
- Prescriptive docs capture how this _type_ of data _should always_ look. Think testing, validation, SLAs. Prescriptive docs are usually compiled from Expectations.

### Organization

The Render module is organized into two parts: ViewModels and SnippetRenderers:
A view_model generates a structured object from an EVR, Expectation Confiugration, or Data Profile, and can optionally apply a template to the structured data.
  - it uses SnippetRenderers to generate a variety of *content blocks*:
TODO: SnippetRenderers should probably be called ContentBlockRenderers
TODO: SnippetRenderers should also use jinja instead of string spanning
- snippets : Each snippet converts a single Expectation (or EVR) to a string or list of strings.
- view_models : View models convert groups of expectations, usually JSON objects. View_models can be nested.



### SnippetRenderers

Each `SnippetRenderer` is a collection of functions that each convert a single Expectation (or EVR) to a serializable string or list of strings.

TODO: Some snippetrenderers should operate on multiple expectation_configurations or EVRs (e.g. bullet list summarizing conditions across columns in a table)

```
ExpectationBulletPointSnippetRenderer.render({
    "expectation_type": "expect_column_to_exist",
    "kwargs": {"column": "x_var"}
}, include_column_name=True)

> "x_var is a required field."
```

The string may include markup.

```
ExpectationBulletPointSnippetRenderer.render({
    "expectation_type": "expect_column_value_lengths_to_be_between",
    "kwargs": {
        "column": "last_name",
        "min_value": 3,
        "max_value": 20,
        "mostly": .95
    }
})

> 'must be between <span class="param-span">3</span> and <span class="param-span">20</span> characters long at least <span class="param-span">0.95</span>% of the time.'
```

The string could also be a JSON object representing a graph or some other object.

```
EvrContentBlockSnippetRenderer.render(my_evr)

> {
  "content_block_type": "graph",
  "content": [
    {
      "$schema": "https://vega.github.io/schema/vega-lite/v2.6.0.json",
      "config": {
        "view": {
          "height": 300,
          "width": 400
        }
      },
      "datasets": {
        "data-cfff8a6fe8134dace707fd67405d0857": [
          {
            "count": 45641,
            "value": "Valid"
          },
          {
            "count": 75,
            "value": "Relict"
          }
        ]
      },
      "height": 900,
      "layer": [
        {
          "data": {
            "name": "data-cfff8a6fe8134dace707fd67405d0857"
          },
          "encoding": {
            "x": {
              "field": "count",
              "type": "quantitative"
            },
            "y": {
              "field": "value",
              "type": "ordinal"
            }
          },
          "height": 80,
          "mark": "bar",
          "width": 240
        },
        {
          "data": {
            "name": "data-cfff8a6fe8134dace707fd67405d0857"
          },
          "encoding": {
            "text": {
              "field": "count",
              "type": "quantitative"
            },
            "x": {
              "field": "count",
              "type": "quantitative"
            },
            "y": {
              "field": "value",
              "type": "ordinal"
            }
          },
          "height": 80,
          "mark": {
            "align": "left",
            "baseline": "middle",
            "dx": 3,
            "type": "text"
          },
          "width": 240
        }
      ]
    }
  ]
}
```

`MySnippetRenderer.render` accepts

{ Expectation or EVR } -> snippet

`view_models`

{ Expectations or EVRs } -> view_model -> view_model ... -> view_model -> snippets

For example, to generate standalone documentation in GE, you can invoke a `PageRenderer` and it will handle the cascade of `SectionRenderers` and `SnippetRenderers` needed to render all the way to HTML. This is the flow shown in the video.

### Code organization

### The `default_html` view_model

`Pages`: render a standalone HTML page from a set of Expectations or EVRs. Their primary business logic (1) groups Expectations into logical buckets and calls the appropriate SectionRenderers, and (2) provides jinja templating for the default rendering engine.

`Sections`: render a section (such as `div`s containing info about a dataframe columns, or a Data Dictionary), by piping Expectations through Snippets and into appropriate ContentBlocks.

`ContentBlocks` provide a basic JSON-to-DOM API for common screen elements for data documentation:

    * headers
    * bullet lists
    * tables
    * graphs
    * etc.

### `view_models`

view_models are a tree

Required methods

.validate()
.render()

Like Expectations themselves, all `view_models` must be return valid, serializable JSON objects.

View models in `view_models.default_html` use the following hierarchy:

```
Page
Section
Content_Block
```

This organization tends to work well for most web displays. It's certainly possible to However, there's nothing speci

For example, `view_models.slack` contains sensible

### `snippets`

The relationship from Expectation (or EVR) to snippet methods is one to one or none.

.render()

### How do I render Expectations to HTML with a `view_model`?

...from within python:

```
ge.render(
    expectation_suite=my_expectation_config,
    view_model=ge.render.view_models.default_html.prescriptive_data_docs
)
```

...from the command line:

### How do I render EVRs to HTML with a `view_model`?

```
ge.render(
    expectation_suite=my_expectation_config,
    view_model=ge.render.view_models.slack
    render_to="json"
)
```

### How do I change styling for rendered content?

You have three main options:

1. Override the CSS
2. Inject classes
3. Subclass the view_model class

### How can I embed rendered content in another?

You have three main options:

1. Render directly to HTML and inject the
2. (If you're using jinja) Compile the appropriate `view_model` to JSON, and reference the default jinja templates from within your own rendering engine.
3. Compile the appropriate `view_model` to JSON, and provide your own view components

### How do I create a custom view_model?

"""
jtbd:

- render many Expectation Suites as a full static HTML site
  navigation

- render a single Expectation Suite as a (potentially nested) list of elements (e.g. div, p, span, JSON, jinja, markdown)
  grouping?

- render a single Expectation as a single element (e.g. div, p, span, JSON, jinja, markdown)
  """
