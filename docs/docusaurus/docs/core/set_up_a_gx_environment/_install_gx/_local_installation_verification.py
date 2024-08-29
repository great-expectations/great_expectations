"""
This is an example script for how to verify the version of the current GX installation.

To test, run: pytest --docs-tests -k "verify_gx_version" tests/integration/test_script_runner.py

"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/set_up_a_gx_environment/_install_gx/_local_installation_verification.py - full code example">
import great_expectations as gx

print(gx.__version__)
# </snippet>
