# -*- coding: utf-8 -*-
from great_expectations import __version__ as ge_version

# !!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.
greeting_1 = """      <cyan>~ Always know what to expect from your data. ~</cyan>

If you're new to Great Expectations, this tutorial is a good place to start:
    - <blue>https://docs.greatexpectations.io/en/latest/getting_started/cli_init.html?utm_source=cli&utm_medium=init&utm_campaign={0:s}</blue>
""".format(ge_version.replace(".", "_"))

msg_prompt_lets_begin = """Let's add Great Expectations to your project, by scaffolding a new great_expectations directory
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
        │   └── local_site
        ├── samples
        └── validations
    
OK to proceed?
"""
