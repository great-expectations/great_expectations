# -*- coding: utf-8 -*-
from great_expectations import rtd_url_ge_version, DataContext

# !!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.
GREETING = """<cyan>
                            _____                _   
                           / ____|              | |  
                          | |  __ _ __ ___  __ _| |_ 
                          | | |_ | '__/ _ \/ _` | __|
                          | |__| | | |  __/ (_| | |_ 
                           \_____|_|  \___|\__,_|\__|
           ______                      _        _   _                 
          |  ____|                    | |      | | (_)                
          | |__  __  ___ __   ___  ___| |_ __ _| |_ _  ___  _ __  ___ 
          |  __| \ \/ / '_ \ / _ \/ __| __/ _` | __| |/ _ \| '_ \/ __|
          | |____ >  <| |_) |  __/ (__| || (_| | |_| | (_) | | | \__ \ 
          |______/_/\_\ .__/ \___|\___|\__\__,_|\__|_|\___/|_| |_|___/
                      | |                                             
                      |_|                                             
                  ~ Always know what to expect from your data ~
</cyan>
New to Great Expectations? Start with this tutorial!
  - <blue>https://docs.greatexpectations.io/en/latest/getting_started/cli_init.html?utm_source=cli&utm_medium=init&utm_campaign={0:s}</blue>
""".format(rtd_url_ge_version)

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
        ├── samples
        └── validations
    
OK to proceed?
"""

PROJECT_IS_COMPLETE = "This looks like an existing project that <green>appears complete!</green> You are <green>ready to roll.</green>\n"

RUN_INIT_AGAIN = "OK. You must run <green>great_expectations init</green> to fix the missing files!"

COMPLETE_ONBOARDING_PROMPT = """To run locally, we need some files that are not in source control.
  - Anything existing will not be modified.
  - Would you like to fix this automatically?"""

ONBOARDING_COMPLETE = """\nDone. You may see new files in `<yellow>great_expectations/uncommitted</yellow>`.
  - Now add secrets to <yellow>great_expectations/uncommitted/config_variables.yml</yellow> to finish onboarding.
"""

BUILD_DOCS_PROMPT = "Would you like to build & view this project's Data Docs!?"

NEW_TEMPLATE_PROMPT = """\nWould you like to install a new config file template?
  - We will move your existing `{}` to `{}`"""

NEW_TEMPLATE_INSTALLED = """\nOK. You now have a new config file: `{}`.
  - Please copy the relevant values from the archived file ({}) into this new template.
"""

NO_DATASOURCES_FOUND = """<red>Error: No datasources were found.</red> Please add one by:
  - running `<green>great_expectations add-datasource</green>` or
  - by editing the {} file""".format(DataContext.GE_YML)

NO_DATASOURCES_FOUND = """You can add add one by:
  - running `<green>great_expectations add-datasource</green>` or
  - by editing the {} file""".format(DataContext.GE_YML)

