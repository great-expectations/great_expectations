"""
Pre-requisites:
- install google-cloud-sdk: https://cloud.google.com/sdk/docs/install or brew install
- create authentication credentials file: https://cloud.google.com/docs/authentication/external/set-up-adc

To run this test, you must set the following environment variables:
GE_TEST_GCP_PROJECT
GE_TEST_BIGQUERY_DATASET

Then run the following command from the great_expectations directory:
```
GE_TEST_GCP_PROJECT=<YOUR GCP PROJECT> GE_TEST_BIGQUERY_DATASET=<YOUR BIGQUERY DATASET> pytest -v --docs-tests -m integration -k "how_to_host_and_share_data_docs_on_gcs" tests/integration/test_script_runner.py --bigquery
```
"""
import os
import subprocess

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()

# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(
        "Environment Variable GE_TEST_GCP_PROJECT is required to run GCS integration tests"
    )
# Although not used explicitly in this test, the following environment variable is required
bigquery_dataset = os.environ.get("GE_TEST_BIGQUERY_DATASET")
if not bigquery_dataset:
    raise ValueError(
        "Environment Variable GE_TEST_BIGQUERY_DATASET is required to run GCS integration tests"
    )

# set GCP project
result = subprocess.run(
    f"gcloud config set project {gcp_project}".split(),
    check=True,
    stderr=subprocess.PIPE,
)

try:
    # remove this bucket if there was a failure in the script last time
    result = subprocess.run(
        "gsutil rm -r gs://superconductive-integration-tests-data-docs".split(),
        check=True,
        stderr=subprocess.PIPE,
    )
except Exception as e:
    pass

create_data_docs_directory = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket command">
gsutil mb -p <YOUR GCP PROJECT NAME> -l US-EAST1 -b on gs://<YOUR GCS BUCKET NAME>/
# </snippet>
"""

# Overwrite the version with the snippet tags
create_data_docs_directory = """
gsutil mb -p <YOUR GCP PROJECT NAME> -l US-EAST1 -b on gs://<YOUR GCS BUCKET NAME>/
"""
create_data_docs_directory = create_data_docs_directory.replace(
    "<YOUR GCP PROJECT NAME>", gcp_project
)
create_data_docs_directory = create_data_docs_directory.replace(
    "<YOUR GCS BUCKET NAME>", "superconductive-integration-tests-data-docs"
)

result = subprocess.run(
    create_data_docs_directory.strip().split(),
    check=True,
    stderr=subprocess.PIPE,
)
stderr = result.stderr.decode("utf-8")

create_data_docs_directory_output = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket output">
Creating gs://<YOUR GCS BUCKET NAME>/...
# </snippet>
"""

# Overwrite the version with the snippet tags
create_data_docs_directory_output = """
Creating gs://<YOUR GCS BUCKET NAME>/...
"""
create_data_docs_directory_output = create_data_docs_directory_output.replace(
    "<YOUR GCS BUCKET NAME>", "superconductive-integration-tests-data-docs"
)

assert create_data_docs_directory_output.strip() in stderr

app_yaml = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py app yaml">
runtime: python37
env_variables:
  CLOUD_STORAGE_BUCKET: <YOUR GCS BUCKET NAME>
# </snippet>
"""

# Overwrite the version with the snippet tags
app_yaml = """
runtime: python37
env_variables:
  CLOUD_STORAGE_BUCKET: <YOUR GCS BUCKET NAME>
"""
app_yaml = app_yaml.replace(
    "<YOUR GCS BUCKET NAME>", "superconductive-integration-tests-data-docs"
)

team_gcs_app_directory = os.path.join(context.root_directory, "team_gcs_app")
os.makedirs(team_gcs_app_directory, exist_ok=True)

app_yaml_file_path = os.path.join(team_gcs_app_directory, "app.yaml")
with open(app_yaml_file_path, "w") as f:
    yaml.dump(app_yaml, f)

requirements_txt = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py requirements.txt">
flask>=1.1.0
google-cloud-storage
# </snippet>
"""

# Overwrite the version with the snippet tags
requirements_txt = """
flask>=1.1.0
google-cloud-storage
"""

requirements_txt_file_path = os.path.join(team_gcs_app_directory, "requirements.txt")
with open(requirements_txt_file_path, "w") as f:
    f.write(requirements_txt)

main_py = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py imports">
import logging
import os
from flask import Flask, request
from google.cloud import storage
app = Flask(__name__)
# Configure this environment variable via app.yaml
CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def index(path):
    gcs = storage.Client()
    bucket = gcs.get_bucket(CLOUD_STORAGE_BUCKET)
    try:
        blob = bucket.get_blob(path)
        content = blob.download_as_string()
        if blob.content_encoding:
            resource = content.decode(blob.content_encoding)
        else:
            resource = content
    except Exception as e:
        logging.exception("couldn't get blob")
        resource = "<p></p>"
    return resource
@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return '''
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    '''.format(e), 500
# </snippet>
"""

main_py_file_path = os.path.join(team_gcs_app_directory, "main.py")
with open(main_py_file_path, "w") as f:
    f.write(main_py)

gcloud_login_command = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud login and set project">
gcloud auth login && gcloud config set project <YOUR GCP PROJECT NAME>
# </snippet>
"""

# Overwrite the version with the snippet tags
gcloud_login_command = """
gcloud auth login && gcloud config set project <YOUR GCP PROJECT NAME>
"""

gcloud_app_deploy_command = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud app deploy">
gcloud app deploy
# </snippet>
"""
# Overwrite the version with the snippet tags
gcloud_app_deploy_command = """
gcloud app deploy
"""

with subprocess.Popen(
    gcloud_app_deploy_command.strip().split(),
    cwd=team_gcs_app_directory,
) as result:
    stdout, stderr = result.communicate()
    result.kill()


data_docs_site_yaml = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py data docs sites yaml">
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: true
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
  new_site_name:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
# </snippet>
"""
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCP PROJECT NAME>", gcp_project
)
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCS BUCKET NAME>", "superconductive-integration-tests-data-docs"
)
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["data_docs_sites"] = yaml.load(data_docs_site_yaml)[
    "data_docs_sites"
]
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

build_data_docs_command = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py build data docs command">
great_expectations docs build --site-name new_site_name
# </snippet>
"""
# Overwrite the version with the snippet tags
build_data_docs_command = """
great_expectations docs build --site-name new_site_name
"""

with subprocess.Popen(
    "echo Y | " + build_data_docs_command.strip() + " --no-view",
    shell=True,
    stdout=subprocess.PIPE,
) as result:
    stdout = result.stdout.read().decode("utf-8")
    result.kill()

build_data_docs_output = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py build data docs output">
The following Data Docs sites will be built:

 - new_site_name: https://storage.googleapis.com/<YOUR GCS BUCKET NAME>/index.html

Would you like to proceed? [Y/n]: Y
Building Data Docs...

Done building Data Docs
# </snippet>
"""

assert (
    "https://storage.googleapis.com/superconductive-integration-tests-data-docs/index.html"
    in stdout
)
assert "Done building Data Docs" in stdout

# remove this bucket to clean up for next time
result = subprocess.run(
    "gsutil rm -r gs://superconductive-integration-tests-data-docs/".split(),
    check=True,
    stderr=subprocess.PIPE,
)
