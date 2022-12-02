from great_expectations.core._docs_decorators import (
    deprecated,
    public_api,
)


@public_api
def func_full_docstring_public_api(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@deprecated(version="1.2.3", message="This is deprecated!!")
def func_full_docstring_deprecated(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


def func_only_summary():
    """My docstring."""
    pass


def func_no_docstring():
    pass


def test_public_api_decorator():
    assert func_full_docstring_public_api.__doc__ == (
        "--Public API--My docstring.\n"
        "\n"
        "    Longer description.\n"
        "\n"
        "    Args:\n"
        "        some_arg: describe some_arg\n"
        "        other_arg: describe other_arg\n"
        "    "
    )
    assert func_full_docstring_public_api.__name__ == "func_full_docstring_public_api"


def test_deprecated_decorator():

    assert func_full_docstring_deprecated.__doc__ == (
        "My docstring.\n"
        "\n"
        ".. deprecated:: 1.2.3\n"
        "    This is deprecated!!\n"
        "\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: describe some_arg\n"
        "    other_arg: describe other_arg\n"
    )
