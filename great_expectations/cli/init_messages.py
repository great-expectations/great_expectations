# -*- coding: utf-8 -*-
from great_expectations import __version__ as ge_version

# !!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.
GREETING = """      <cyan>~ Always know what to expect from your data. ~</cyan>

If you're new to Great Expectations, this tutorial is a good place to start:
  - <blue>https://docs.greatexpectations.io/en/latest/getting_started/cli_init.html?utm_source=cli&utm_medium=init&utm_campaign={0:s}</blue>
""".format(ge_version.replace(".", "_"))

LETS_BEGIN_PROMPT = """Let's add Great Expectations to your project, by scaffolding a new great_expectations directory
that will look like this:

    great_expectations
    ├── .gitignore
    ├── datasources
    ├── expectations
    ├── great_expectations.yml
    ├── notebooks
    │   ├── create_expectations.ipynb
    │   └── integrate_validation_into_pipeline.ipynb
    ├── plugins
    └── uncommitted
        ├── config_variables.yml
        ├── data_docs
        │   └── local_site
        ├── samples
        └── validations
    
OK to proceed?
"""

PROJECT_IS_COMPLETE = "This looks like an existing project that <green>appears complete!</green> You are <green>ready to roll.</green>\n"

CONTINUE_ONBOARDING = """This looks like an existing project that is not quite ready. <green>Let's continue your onboarding!</green>
  - The existing <yellow>{}great_expectations.yml</yellow> will not be modified.
"""

FIX_MISSING_DIRS_PROMPT = """Great Expectations needs some directories that are not in source control.
  - Would you like to create any that are missing?
"""
DIRS_FIXED = """\nDone. You may see new directories in `<yellow>great_expectations/uncommitted</yellow>`.
  - Now add secrets to <yellow>great_expectations/uncommitted/config_variables.yml</yellow> to finish onboarding.
"""
SUGGESTED_ACTIONS = """Some other things you may want to do:
  - Run '<green>great_expectations build-docs</green>` to see your teammates Expectations!
  - Run '<green>great_expectations add-datasource</green>` to add a new datasource.
  - Run '<green>great_expectations profile</green>` to profile some data."""

