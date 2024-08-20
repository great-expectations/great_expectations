"""
This is an example script for how to toggle Analytics Events using Data Context variables.

To test, run:
pytest --docs-tests -k "docs_example_toggle_analytics_events" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - full code example">
import great_expectations as gx

# Get a File Data Context:
context = gx.get_context(mode="file")
# Hide this
set_up_context_for_example(context)

# Set the `analytics_enabled` Data Context variable:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - enable Analytics Events">
context.variables.analytics_enabled = True
# </snippet>
# or:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - disable Analytics Events">
context.variables.analytics_enabled = False
# </snippet>

# Save the change to the Data Context's config file:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - save changes to the Data Context">
context.variables.save()
# </snippet>

# Re-initialize the Data Context using the updated
# `analytics_enabled` configuration:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - re-initialize the Data Context">
context = gx.get_context(mode="file")
# </snippet>
# </snippet>

# Hide this
assert not context.variables.analytics_enabled
