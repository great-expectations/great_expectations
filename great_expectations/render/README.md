# Introduction to Expectation Renderers

### Why Renderers?

### Key concepts

### Code organization

{ Expectations or EVRs } -> view_model -> view_model ... -> view_model -> snippets
{ Expectation or EVR } -> snippet

By convention, the default view_models have the following hierarchy:
Page
Section
Content_Block

This organization tends to work well for most web displays. It's certainly possible to

Slack renderer

view_models are a tree
The relationship from Expectation (or EVR) to view_models methods is one to one or none.

Like Expectations themselves, all `view_models` must be return valid JSON objects.

### `view_models`

### `snippets`

### How do I render an object with a view_model?

ge.render_view_model(
expectations_config=my_expectation_config,
view_model=ge.render.view_models.default_html
render_to="json"
)

ge.render_view_model(
expectations_config=my_expectation_config,
view_model=ge.render.view_models.slack
render_to="json"
)

### How do I invoke an existing view_model?

### How do I add a new view_model?
