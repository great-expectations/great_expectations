import pytest

pytestmarks = pytest.mark.filesystem


def read_config_from_file(config_filename):
    with open(config_filename) as f_:
        config = f_.read()

    return config
