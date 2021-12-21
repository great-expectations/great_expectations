import os
import subprocess

from ruamel import yaml

import great_expectations as ge

context = ge.get_context()

# set GCP project
result = subprocess.run(
    "gcloud config set project superconductive-internal".split(),
    check=True,
    stderr=subprocess.PIPE,
)

try:
    # remove this bucket if there was a failure in the script last time
    result = subprocess.run(
        "gsutil rm -r gs://superconductive-integration-tests-data-docs/".split(),
        check=True,
        stderr=subprocess.PIPE,
    )
except Exception as e:
    pass

create_data_docs_directory = """
gsutil mb -p <YOUR GCP PROJECT NAME> -l US-EAST1 -b on gs://<YOUR GCS BUCKET NAME>/
"""
create_data_docs_directory = create_data_docs_directory.replace(
    "<YOUR GCP PROJECT NAME>", "superconductive-internal"
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
assert "Creating gs://superconductive-integration-tests-data-docs/..." in stderr


app_yaml = """
  # app.yaml (make sure to use your own bucket name)

  runtime: python37
  env_variables:
      CLOUD_STORAGE_BUCKET: my_org_data_docs
"""

requirements_txt = """
  # requirements.txt

  flask>=1.1.0
  google-cloud-storage
"""

main_py = """
  # main.py

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
"""

gcloud_login_command = """
  # Insert the appropriate project name.

  gcloud auth login && gcloud config set project <<project_name>>
"""

data_docs_site_yaml = """
  data_docs_sites:
    local_site:
      class_name: SiteBuilder
      show_how_to_buttons: true
      store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
    gs_site:  # this is a user-selected name - you may select your own
      class_name: SiteBuilder
      store_backend:
        class_name: TupleGCSStoreBackend
        project: my_org_project # UPDATE the project name with your own
        bucket: my_org_data_docs  # UPDATE the bucket name here to match the bucket you configured above
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
"""

build_data_docs_command = """
  great_expectations --v3-api docs build --site-name gs_site

  The following Data Docs sites will be built:

   - gs_site: https://storage.googleapis.com/my_org_data_docs/index.html

  Would you like to proceed? [Y/n]: Y

  Building Data Docs...

  Done building Data Docs
"""

# remove this bucket to clean up for next time
result = subprocess.run(
    "gsutil rm -r gs://superconductive-integration-tests-data-docs/".split(),
    check=True,
    stderr=subprocess.PIPE,
)
