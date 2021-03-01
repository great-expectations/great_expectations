from unittest.mock import patch

from great_expectations.cli.datasource import _collect_ibm_db2_credentials


@patch("click.prompt")
def test_ibm_db2_credentials(mock_prompt):
    mock_prompt.side_effect = [
        "my_db2_server",
        "12345",
        "my_db2_username",
        "my_db2_password",
        "my_db2_database_name",
    ]

    credentials = _collect_ibm_db2_credentials(None)

    assert credentials == {
        "drivername": "db2+ibm_db",
        "database": "my_db2_database_name",
        "host": "my_db2_server",
        "password": "my_db2_password",
        "username": "my_db2_username",
        "port": "12345",
    }
