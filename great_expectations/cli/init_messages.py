# -*- coding: utf-8 -*-
from great_expectations import rtd_url_ge_version, DataContext

# !!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.

GREETING = """<cyan>\
  ___              _     ___                  _        _   _             
 / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___
| (_ | '_/ -_) _` |  _| | _|\ \ / '_ \/ -_) _|  _/ _` |  _| / _ \ ' \(_-<
 \___|_| \___\__,_|\__| |___/_\_\ .__/\___\__|\__\__,_|\__|_\___/_||_/__/
                                |_|                                      
             ~ Always know what to expect from your data ~             
</cyan>""".format(rtd_url_ge_version)

LETS_BEGIN_PROMPT = """Let's add Great Expectations to your project by creating a new directory:

    great_expectations
    ├── expectations
    ├── great_expectations.yml
    ├── notebooks
    │   ├── pandas
    │   ├── spark
    │   └── sql
    ├── plugins
    │   └── ...
    └── uncommitted
        ├── config_variables.yml
        └── ...
OK to proceed?"""

PROJECT_IS_COMPLETE = "This looks like an existing project that <green>appears complete!</green> You are <green>ready to roll.</green>\n"

RUN_INIT_AGAIN = "OK. You must run <green>great_expectations init</green> to fix the missing files!"

COMPLETE_ONBOARDING_PROMPT = """To run locally, we need some files that are not in source control.
  - Anything existing will not be modified.
  - Would you like to fix this automatically?"""

SLACK_SETUP_INTRO = """
<cyan>========== Slack Notifications ==========</cyan>
""".format(rtd_url_ge_version)

SLACK_SETUP_PROMPT = "Would you like to set up Slack data quality notifications?"

SLACK_DOC_LINK = """http://docs.greatexpectations.io/en/latest/getting_started/cli_init.html#configuring-slack-notifications
"""

SLACK_WEBHOOK_PROMPT = """Please add your Slack webhook below. Getting one is easy!
"""

SLACK_LATER = "\nTo setup Slack later please see the the slack section in the CLI init getting started guide."

SLACK_SETUP_COMPLETE = """
OK. <green>Slack is set up.</green> To modify this in the future please see the slack section in the CLI init getting started guide."""

ONBOARDING_COMPLETE = """
Done. You may see new files in `<yellow>great_expectations/uncommitted</yellow>`.
  - Now add secrets to <yellow>great_expectations/uncommitted/config_variables.yml</yellow> to finish onboarding.
"""

BUILD_DOCS_PROMPT = "Would you like to build & view this project's Data Docs!?"

NEW_TEMPLATE_PROMPT = """
Would you like to install a new config file template?
  - We will move your existing `{}` to `{}`"""

NEW_TEMPLATE_INSTALLED = """
OK. You now have a new config file: `{}`.
  - Please copy the relevant values from the archived file ({}) into this new template.
"""

NO_DATASOURCES_FOUND = """<red>Error: No datasources were found.</red> Please add one by:
  - running `<green>great_expectations add-datasource</green>` or
  - by editing the {} file
""".format(DataContext.GE_YML)
