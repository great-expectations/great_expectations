YAML files make variables more visible, are easier to edit, and allow for modularization. For example, you can create a YAML file for development and testing and another for production.

A File Data Context is required before you can configure credentials in a YAML file.  By default, the credentials file in a File Data Context is located at `/great_expectations/uncommitted/config_variables.yml`

1. Save your access credentials or the database connection string to ``great_expectations/uncommitted/config_variables.yml``. For example:

    ```yaml title="YAML" name="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials.py config_variables_yaml"
    ```
    To store values that include the dollar sign character ``$``, escape them using a backslash ``\`` to avoid substitution issues. For example, in the previous example for Postgres credentials you'd set ``password: pa\$sword`` if your password is ``pa$sword``. You can also have multiple substitutions for the same item. For example ``database_string: ${USER}:${PASSWORD}@${HOST}:${PORT}/${DATABASE}``.


2. Run the following code to use the `connection_string` parameter values when you add a `datasource` to a Data Context:

    ```python title="Python" name="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials.py add_credential_from_yml"
    ```