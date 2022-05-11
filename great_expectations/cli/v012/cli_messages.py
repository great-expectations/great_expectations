
from great_expectations import DataContext
GREETING = "<cyan>\n  ___              _     ___                  _        _   _\n / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___\n| (_ | '_/ -_) _` |  _| | _|\\ \\ / '_ \\/ -_) _|  _/ _` |  _| / _ \\ ' \\(_-<\n \\___|_| \\___\\__,_|\\__| |___/_\\_\\ .__/\\___\\__|\\__\\__,_|\\__|_\\___/_||_/__/\n                                |_|\n             ~ Always know what to expect from your data ~\n</cyan>"
LETS_BEGIN_PROMPT = "Let's configure a new Data Context.\n\nFirst, Great Expectations will create a new directory:\n\n    great_expectations\n    |-- great_expectations.yml\n    |-- expectations\n    |-- checkpoints\n    |-- plugins\n    |-- .gitignore\n    |-- uncommitted\n        |-- config_variables.yml\n        |-- documentation\n        |-- validations\n\nOK to proceed?"
PROJECT_IS_COMPLETE = 'This looks like an existing project that <green>appears complete!</green> You are <green>ready to roll.</green>\n'
RUN_INIT_AGAIN = 'OK. You must run <green>great_expectations init</green> to fix the missing files!'
COMPLETE_ONBOARDING_PROMPT = 'To run locally, we need some files that are not in source control.\n  - Anything existing will not be modified.\n  - Would you like to fix this automatically?'
SLACK_SETUP_INTRO = '\n<cyan>========== Slack Notifications ==========</cyan>\n'
SLACK_SETUP_PROMPT = 'Would you like to set up Slack data quality notifications?'
SLACK_DOC_LINK = 'http://docs.greatexpectations.io/en/latest/getting_started/cli_init.html#configuring-slack-notifications\n'
SLACK_WEBHOOK_PROMPT = 'Please add your Slack webhook below. Getting one is easy!\n'
SLACK_LATER = '\nTo setup Slack later please see the the slack section in the CLI init getting started guide.'
SLACK_SETUP_COMPLETE = '\nOK. <green>Slack is set up.</green> To modify this in the future please see the slack section in the CLI init getting started guide.'
ONBOARDING_COMPLETE = '\nGreat Expectations added some missing files required to run.\n  - You may see new files in `<yellow>great_expectations/uncommitted</yellow>`.\n  - You may need to add secrets to `<yellow>great_expectations/uncommitted/config_variables.yml</yellow>` to finish onboarding.\n'
BUILD_DOCS_PROMPT = "Would you like to build & view this project's Data Docs!?"
NO_DATASOURCES_FOUND = '<red>Error: No datasources were found.</red> Please add one by:\n  - running `<green>great_expectations datasource new</green>` or\n  - by editing the {} file\n'.format(DataContext.GE_YML)
SETUP_SUCCESS = '\n<cyan>Congratulations! Great Expectations is now set up.</cyan>'
SECTION_SEPARATOR = '\n================================================================================\n'
DONE = 'Done'
