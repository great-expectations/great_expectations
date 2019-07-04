import pytest

import os

@pytest.fixture()
def data_context_config_string():
    config_str = \
"""
# This is a comment
# it should be preserved.
datasources:
  # this comments should also be preserved
  default:
    type: pandas
    generators:
      # The name default is read if no datasource or generator is specified
      default:
        type: filesystem
        base_dir: /data
"""

def test_preserve_comments(data_context):
    data_context.add_datasource("test_datasource", "pandas")

    context_root_dir = data_context.root_directory

    with open(os.path.join(context_root_dir, "great_expectations.yml"), "r") as infile:
        lines = infile.readlines()

    assert lines[0] == "# This is a basic configuration for testing.\n"
    assert lines[2] == "datasources:\n"
    assert lines[3] == "  # For example, this one.\n"