import os
import glob
import shutil

from great_expectations import script_relative_path


def safe_mmkdir(directory):
    try:
        os.mkdir(directory)
    except FileExistsError as fe:
        pass


def _does_user_want(user_input):
    while user_input.lower() not in ["y", "yes", "no", "n", ""]:
        user_input = input("[Y/n] is required. Please try again. ")

    return user_input.lower() in ["", "yes", "y", "yes"]
    # return user_input.lower() not in ["no", "n", "false", "f"]


def _save_append_line_to_gitignore(line):
    _gitignore = ".gitignore"
    if os.path.exists(_gitignore):
        append_write = 'a'
    else:
        append_write = 'w'

    with open(_gitignore, append_write) as gitignore:
        gitignore.write(line + "\n")


def _profile_template():
    return """
superconductive:
    default:
        type: postgres
        host: localhost 
        port: 5432
        user: postgres  
        pass: "****"
        dbname: postgres
"""


def _yml_template(bucket="''", slack_webhook="''", sql_alchemy_profile="YOUR_SQLALCHEMY_PROFILE", dbt_profile="YOUR_DBT_PROFILE"):
    return """# This project file was created with the command `great_expectations init`

aws:
  # Add the name of an S3 bucket here. Validation reports and datasets can be 
  # stored here for easy debugging.
  bucket: {}

# Add your Slack webhook here to get notifications of validation results
# See https://api.slack.com/incoming-webhooks for setup
slack_webhook: {}

# Configure datasources below. Valid datasource types include pandas, sqlalchemy, and dbt
datasources:
  mycsvfile:
    type: pandas
  mydb:
    type: sqlalchemy
    profile_name: {}
    profiles_filepath: ~/.great_expectations/profiles.yml
  mydbt:
    type: dbt
    profile: {} 
    profiles_filepath: ~/.dbt/profiles.yml
""".format(bucket, slack_webhook, sql_alchemy_profile, dbt_profile)


def _scaffold_directories_and_notebooks(base_dir):
    safe_mmkdir(base_dir)
    notebook_dir_name = "notebooks"

    open(os.path.join(base_dir, ".gitignore"), 'w').write("""do_not_commit/""")

    for directory in [notebook_dir_name, "expectation_configs", "validations", "snapshots", "samples", "do_not_commit"]:
        safe_mmkdir(os.path.join(base_dir, directory))

    for notebook in glob.glob(script_relative_path("../init_notebooks/*.ipynb")):
        notebook_name = os.path.basename(notebook)
        shutil.copyfile(notebook, os.path.join(
            base_dir, notebook_dir_name, notebook_name))

    safe_mmkdir(os.path.join(base_dir, notebook_dir_name, "tutorial_data"))
    shutil.copyfile(script_relative_path("../init_notebooks/tutorial_data/Titanic.csv"),
                    os.path.join(base_dir, notebook_dir_name, "tutorial_data", "Titanic.csv"))
