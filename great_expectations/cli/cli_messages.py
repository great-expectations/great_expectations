
from great_expectations import DataContext
GREETING = "<cyan>\n  ___              _     ___                  _        _   _\n / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___\n| (_ | '_/ -_) _` |  _| | _|\\ \\ / '_ \\/ -_) _|  _/ _` |  _| / _ \\ ' \\(_-<\n \\___|_| \\___\\__,_|\\__| |___/_\\_\\ .__/\\___\\__|\\__\\__,_|\\__|_\\___/_||_/__/\n                                |_|\n             ~ Always know what to expect from your data ~\n</cyan>"
LETS_BEGIN_PROMPT = "Let's create a new Data Context to hold your project configuration.\n\nGreat Expectations will create a new directory with the following structure:\n\n    great_expectations\n    |-- great_expectations.yml\n    |-- expectations\n    |-- checkpoints\n    |-- plugins\n    |-- .gitignore\n    |-- uncommitted\n        |-- config_variables.yml\n        |-- data_docs\n        |-- validations\n\nOK to proceed?"
PROJECT_IS_COMPLETE = 'This looks like an existing project that <green>appears complete!</green> You are <green>ready to roll.</green>\n'
RUN_INIT_AGAIN = 'OK. You must run <green>great_expectations init</green> to fix the missing files!'
COMPLETE_ONBOARDING_PROMPT = '\nIt looks like you have a partially initialized Great Expectations project. Would you like to fix this automatically by adding the following missing files (existing files will not be modified)?\n\n   great_expectations\n    |-- plugins\n    |-- uncommitted\n'
ONBOARDING_COMPLETE = '\nGreat Expectations added some missing files required to run.\n  - You may see new files in `<yellow>great_expectations/uncommitted</yellow>`.\n  - You may need to add secrets to `<yellow>great_expectations/uncommitted/config_variables.yml</yellow>` to finish onboarding.\n'
READY_FOR_CUSTOMIZATION = '<cyan>Congratulations! You are now ready to customize your Great Expectations configuration.</cyan>'
HOW_TO_CUSTOMIZE = f'''
<cyan>You can customize your configuration in many ways. Here are some examples:</cyan>

  <cyan>Use the CLI to:</cyan>
    - Run `<green>great_expectations datasource new</green>` to connect to your data.
    - Run `<green>great_expectations checkpoint new <checkpoint_name></green>` to bundle data with Expectation Suite(s) in a Checkpoint for later re-validation.
    - Run `<green>great_expectations suite --help</green>` to create, edit, list, profile Expectation Suites.
    - Run `<green>great_expectations docs --help</green>` to build and manage Data Docs sites.

  <cyan>Edit your configuration in {DataContext.GE_YML} to:</cyan>
    - Move Stores to the cloud
    - Add Slack notifications, PagerDuty alerts, etc.
    - Customize your Data Docs

<cyan>Please see our documentation for more configuration options!</cyan>
'''
SECTION_SEPARATOR = '\n================================================================================\n'
