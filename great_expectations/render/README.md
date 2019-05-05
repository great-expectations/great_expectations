# Introduction to Expectation Renderers

### Why Renderers?

Renderers compile Expectations and Expectation Validation Results (EVRs) to human-readable docs.

This compile step allows Expectations to keep your documentation in sync with your tests. As long as you're consistently running pipeline tests, you know that your docs will never go stale.

Great Expectation's renderers are designed to allow for great flexibility and extensibility. We know that there are many ways to describe and visualize data. Eventually, we hope that GE can encompass them all.

### Key concepts

`snippets`

{ Expectation or EVR } -> snippet

`view_models`

{ Expectations or EVRs } -> view_model -> view_model ... -> view_model -> snippets

For example, to generate standalone documentation in GE, you can invoke a `PageRenderer` and it will handle the cascade of `SectionRenderers` and `SnippetRenderers` needed to render all the way to HTML. This is the flow shown in the video.

`Descriptive` vs `Prescriptive` documentation:

- Descriptive docs capture how _this_ data looks _now_. Think profiling, exploration, summarization.
- Prescriptive docs capture how this _type_ of data _should always_ look. Think testing, validation, SLAs.

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
    expectations_config=my_expectation_config,
    view_model=ge.render.view_models.default_html.prescriptive_data_docs
)
```

...from the command line:

### How do I render EVRs to HTML with a `view_model`?

```
ge.render(
    expectations_config=my_expectation_config,
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
