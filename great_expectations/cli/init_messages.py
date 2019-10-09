# -*- coding: utf-8 -*-
from great_expectations import rtd_url_ge_version

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

CONTINUE_ONBOARDING = """This looks like an existing project that is not quite ready. <green>Let's continue your onboarding!</green>
  - The existing <yellow>{}great_expectations.yml</yellow> will not be modified.
"""

RUN_INIT_AGAIN = "OK. You must run <green>great_expectations init</green> to fix the missing files!"

COMPLETE_ONBOARDING_PROMPT = """Great Expectations needs some files that are not in source control.
  - Would you like to fix this automatically?
"""

ONBOARDING_COMPLETE = """\nDone. You may see new files in `<yellow>great_expectations/uncommitted</yellow>`.
  - Now add secrets to <yellow>great_expectations/uncommitted/config_variables.yml</yellow> to finish onboarding.
"""

BUILD_DOCS_PROMPT = "Good news - someone built some expectations. " \
                    "Would you like to build and view Data Docs!?"
