import pytest

from great_expectations.core._docs_decorators import (
    deprecated,
    public_api,
    version_added,
    deprecated_argument,
    new_argument,
)

# @public_api


@public_api
def _func_full_docstring_public_api(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@public_api
def _func_only_summary_public_api():
    """My docstring."""
    pass


@public_api
def _func_no_docstring_public_api():
    pass


def test_public_api_decorator_full_docstring():
    assert _func_full_docstring_public_api.__doc__ == (
        "--Public API--My docstring.\n"
        "\n"
        "    Longer description.\n"
        "\n"
        "    Args:\n"
        "        some_arg: describe some_arg\n"
        "        other_arg: describe other_arg\n"
        "    "
    )
    assert _func_full_docstring_public_api.__name__ == "_func_full_docstring_public_api"


def test_public_api_decorator_only_summary():
    assert _func_only_summary_public_api.__doc__ == "--Public API--My docstring."
    assert _func_only_summary_public_api.__name__ == "_func_only_summary_public_api"


def test_public_api_decorator_no_docstring():
    assert _func_no_docstring_public_api.__doc__ == "--Public API--"
    assert _func_no_docstring_public_api.__name__ == "_func_no_docstring_public_api"


# @deprecated


@deprecated(version="1.2.3", message="This is deprecated!!")
def _func_full_docstring_deprecated(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@deprecated(version="1.2.3")
def _func_full_docstring_deprecated_no_message(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@deprecated(version="1.2.3", message="This is deprecated!!")
def _func_only_summary_deprecated(some_arg, other_arg):
    """My docstring."""
    pass


@deprecated(version="1.2.3", message="This is deprecated!!")
def _func_no_docstring_deprecated(some_arg, other_arg):
    pass


def test_deprecated_decorator_full_docstring():

    assert _func_full_docstring_deprecated.__doc__ == (
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


def test_deprecated_decorator_full_docstring_no_message():

    assert _func_full_docstring_deprecated_no_message.__doc__ == (
        "My docstring.\n"
        "\n"
        ".. deprecated:: 1.2.3\n"
        "    \n"
        "\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: describe some_arg\n"
        "    other_arg: describe other_arg\n"
    )


def test_deprecated_decorator_only_summary():

    assert _func_only_summary_deprecated.__doc__ == (
        "My docstring.\n" "\n" ".. deprecated:: 1.2.3\n" "    This is deprecated!!\n"
    )


def test_deprecated_decorator_no_docstring():

    assert _func_no_docstring_deprecated.__doc__ == (
        "\n" "\n" ".. deprecated:: 1.2.3\n" "    This is deprecated!!\n"
    )


# @version_added


@version_added(version="1.2.3", message="Added in version 1.2.3")
def _func_full_docstring_version_added(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@version_added(version="1.2.3")
def _func_full_docstring_version_added_no_message(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@version_added(version="1.2.3", message="Added in version 1.2.3")
def _func_only_summary_version_added(some_arg, other_arg):
    """My docstring."""
    pass


@version_added(version="1.2.3", message="Added in version 1.2.3")
def _func_no_docstring_version_added(some_arg, other_arg):
    pass


def test_version_added_decorator_full_docstring():

    assert _func_full_docstring_version_added.__doc__ == (
        "My docstring.\n"
        "\n"
        ".. versionadded:: 1.2.3\n"
        "    Added in version 1.2.3\n"
        "\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: describe some_arg\n"
        "    other_arg: describe other_arg\n"
    )


def test_version_added_decorator_full_docstring_no_message():

    assert _func_full_docstring_version_added_no_message.__doc__ == (
        "My docstring.\n"
        "\n"
        ".. versionadded:: 1.2.3\n"
        "    \n"
        "\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: describe some_arg\n"
        "    other_arg: describe other_arg\n"
    )


def test_version_added_decorator_only_summary():

    assert _func_only_summary_version_added.__doc__ == (
        "My docstring.\n"
        "\n"
        ".. versionadded:: 1.2.3\n"
        "    Added in version 1.2.3\n"
    )


def test_version_added_decorator_no_docstring():

    assert _func_no_docstring_version_added.__doc__ == (
        "\n" "\n" ".. versionadded:: 1.2.3\n" "    Added in version 1.2.3\n"
    )


# All Method Level Decorators


@public_api
@version_added(version="1.2.3", message="Added in version 1.2.3")
@deprecated(version="1.2.3", message="This is deprecated!!")
def _func_full_docstring_all_methoddecorators(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


def test_all_method_decorators_full_docstring():

    assert _func_full_docstring_all_methoddecorators.__doc__ == (
        "--Public API--My docstring.\n"
        "\n"
        ".. versionadded:: 1.2.3\n"
        "    Added in version 1.2.3\n"
        "\n"
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


# @deprecated_argument


@deprecated_argument(argument_name="some_arg", version="1.2.3", message="some msg")
def _func_full_docstring_deprecated_argument(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


def test_deprecated_decorator_full_docstring_deprecated_argument():
    assert _func_full_docstring_deprecated_argument.__doc__ == (
        "My docstring.\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: describe some_arg\n"
        "        .. deprecated:: 1.2.3\n"
        "            some msg\n"
        "    other_arg: describe other_arg"
    )


@deprecated_argument(argument_name="some_arg", version="1.2.3", message="some msg")
def _func_full_docstring_deprecated_argument_no_description(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg:
        other_arg: describe other_arg
    """
    pass


def test_deprecated_decorator_full_docstring_deprecated_argument_no_description():
    assert _func_full_docstring_deprecated_argument_no_description.__doc__ == (
        "My docstring.\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: \n"
        "        .. deprecated:: 1.2.3\n"
        "            some msg\n"
        "    other_arg: describe other_arg"
    )


def test_deprecated_decorator_full_docstring_deprecated_argument_missing():
    with pytest.raises(ValueError) as e:

        @deprecated_argument(
            argument_name="this_arg_doesnt_exist", version="1.2.3", message="some msg"
        )
        def _func_full_docstring_deprecated_argument_missing(some_arg, other_arg):
            """My docstring.

            Longer description.

            Args:
                some_arg: describe some_arg
                other_arg: describe other_arg
            """
            pass

    assert (
        "Please specify an existing argument, you specified this_arg_doesnt_exist."
        in str(e.value)
    )


# @new_argument


@new_argument(argument_name="some_arg", version="1.2.3", message="some msg")
def _func_full_docstring_new_argument(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


def test_new_argument_decorator_full_docstring_new_argument():
    assert _func_full_docstring_new_argument.__doc__ == (
        "My docstring.\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: describe some_arg\n"
        "        .. versionadded:: 1.2.3\n"
        "            some msg\n"
        "    other_arg: describe other_arg"
    )


@new_argument(argument_name="some_arg", version="1.2.3", message="some msg")
def _func_full_docstring_new_argument_no_description(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg:
        other_arg: describe other_arg
    """
    pass


def test_new_argument_full_docstring_new_argument_no_description():
    assert _func_full_docstring_new_argument_no_description.__doc__ == (
        "My docstring.\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: \n"
        "        .. versionadded:: 1.2.3\n"
        "            some msg\n"
        "    other_arg: describe other_arg"
    )


def test_new_argument_full_docstring_new_argument_missing():
    with pytest.raises(ValueError) as e:

        @new_argument(
            argument_name="this_arg_doesnt_exist", version="1.2.3", message="some msg"
        )
        def _func_full_docstring_new_argument_missing(some_arg, other_arg):
            """My docstring.

            Longer description.

            Args:
                some_arg: describe some_arg
                other_arg: describe other_arg
            """
            pass

    assert (
        "Please specify an existing argument, you specified this_arg_doesnt_exist."
        in str(e.value)
    )


# All Decorators


@public_api
@version_added(version="1.2.3", message="Added in version 1.2.3")
@deprecated(version="1.2.3", message="This is deprecated!!")
@new_argument(argument_name="some_arg", version="1.2.3", message="some msg")
@deprecated_argument(argument_name="other_arg", version="1.2.3", message="some msg")
def _func_full_docstring_all_decorators(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


def test_all_decorators_full_docstring():

    assert _func_full_docstring_all_decorators.__doc__ == (
        "--Public API--My docstring.\n"
        "\n"
        ".. versionadded:: 1.2.3\n"
        "    Added in version 1.2.3\n"
        "\n"
        "\n"
        ".. deprecated:: 1.2.3\n"
        "    This is deprecated!!\n"
        "\n"
        "\n"
        "Longer description.\n"
        "\n"
        "Args:\n"
        "    some_arg: describe some_arg\n"
        "        .. versionadded:: 1.2.3\n"
        "            some msg\n"
        "    other_arg: describe other_arg\n"
        "        .. deprecated:: 1.2.3\n"
        "        some msg"
    )
