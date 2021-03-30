from great_expectations import DataContext

GREETING = r"""<cyan>
  ___              _     ___                  _        _   _
 / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___
| (_ | '_/ -_) _` |  _| | _|\ \ / '_ \/ -_) _|  _/ _` |  _| / _ \ ' \(_-<
 \___|_| \___\__,_|\__| |___/_\_\ .__/\___\__|\__\__,_|\__|_\___/_||_/__/
                                |_|
             ~ Always know what to expect from your data ~
</cyan>"""

LETS_BEGIN_PROMPT = """Let's create a new Data Context to hold your project configuration.

Great Expectations will create a new directory with the following structure:

    great_expectations
    |-- great_expectations.yml
    |-- expectations
    |-- checkpoints
    |-- notebooks
    |-- plugins
    |-- .gitignore
    |-- uncommitted
        |-- config_variables.yml
        |-- documentation
        |-- validations

OK to proceed?"""

PROJECT_IS_COMPLETE = "This looks like an existing project that <green>appears complete!</green> You are <green>ready to roll.</green>\n"

RUN_INIT_AGAIN = (
    "OK. You must run <green>great_expectations init</green> to fix the missing files!"
)

COMPLETE_ONBOARDING_PROMPT = """
It looks like you have a partially initialized Great Expectations project. Would you like to fix this automatically by adding the missing files (existing files will not be modified)?"""

SLACK_SETUP_INTRO = """
<cyan>========== Slack Notifications ==========</cyan>
"""

SLACK_SETUP_PROMPT = "Would you like to set up Slack data quality notifications?"

SLACK_DOC_LINK = """http://docs.greatexpectations.io/en/latest/getting_started/cli_init.html#configuring-slack-notifications
"""

SLACK_WEBHOOK_PROMPT = """Please add your Slack webhook below. Getting one is easy!
"""

SLACK_LATER = "\nTo setup Slack later please see the the slack section in the CLI init getting started guide."

SLACK_SETUP_COMPLETE = """
OK. <green>Slack is set up.</green> To modify this in the future please see the slack section in the CLI init getting started guide."""

ONBOARDING_COMPLETE = """
Great Expectations added some missing files required to run.
  - You may see new files in `<yellow>great_expectations/uncommitted</yellow>`.
  - You may need to add secrets to `<yellow>great_expectations/uncommitted/config_variables.yml</yellow>` to finish onboarding.
"""

NO_DATASOURCES_FOUND = """<red>Error: No datasources were found.</red> Please add one by:
  - running `<green>great_expectations datasource new</green>` or
  - by editing the {} file
""".format(
    DataContext.GE_YML
)

READY_FOR_CUSTOMIZATION = """<cyan>Congratulations! You are now ready to customize your Great Expectations configuration.</cyan>"""

HOW_TO_CUSTOMIZE = f"""\n<cyan>You can customize your configuration in many ways. Here are some examples:</cyan>

  <cyan>Use the CLI to:</cyan>
    - Run `<green>great_expectations datasource new</green>` to connect to your data
    - Run `<green>great_expectations checkpoint new <checkpoint_name></green>` to bundle data with Expectation Suite(s) in a Checkpoint definition for later re-validation
    - Create, edit, list, profile Expectation Suites
    - Manage and customize Data Docs, Stores

  <cyan>Edit your configuration in {DataContext.GE_YML} to:</cyan>
    - Move Stores to the cloud
    - Add Slack notifications, PagerDuty alerts, etc.
    - Customize your Data Docs

<cyan>Please see our documentation for more configuration options!</cyan>
"""

SECTION_SEPARATOR = "\n================================================================================\n"

DONE = "Done"
