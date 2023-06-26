import pytest

from great_expectations.core._docs_decorators import (
    deprecated_argument,
    deprecated_method_or_class,
    new_argument,
    new_method_or_class,
    public_api,
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


class TestPublicAPI:
    @pytest.mark.unit
    def test_public_api_decorator_full_docstring(self):
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
        assert (
            _func_full_docstring_public_api.__name__
            == "_func_full_docstring_public_api"
        )

    @pytest.mark.unit
    def test_public_api_decorator_only_summary(self):
        assert _func_only_summary_public_api.__doc__ == "--Public API--My docstring."
        assert _func_only_summary_public_api.__name__ == "_func_only_summary_public_api"

    @pytest.mark.unit
    def test_public_api_decorator_no_docstring(self):
        assert _func_no_docstring_public_api.__doc__ == "--Public API--"
        assert _func_no_docstring_public_api.__name__ == "_func_no_docstring_public_api"


# @deprecated


@deprecated_method_or_class(version="1.2.3", message="This is deprecated!!")
def _func_full_docstring_deprecated(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@deprecated_method_or_class(version="1.2.3")
def _func_full_docstring_deprecated_no_message(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@deprecated_method_or_class(version="1.2.3", message="This is deprecated!!")
def _func_only_summary_deprecated(some_arg, other_arg):
    """My docstring."""
    pass


@deprecated_method_or_class(version="1.2.3", message="This is deprecated!!")
def _func_no_docstring_deprecated(some_arg, other_arg):
    pass


class TestDeprecatedMethod:
    @pytest.mark.unit
    def test_deprecated_decorator_full_docstring(self):
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
        assert (
            _func_full_docstring_deprecated.__name__
            == "_func_full_docstring_deprecated"
        )

    @pytest.mark.unit
    def test_deprecated_decorator_full_docstring_no_message(self):
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

    @pytest.mark.unit
    def test_deprecated_decorator_only_summary(self):
        assert _func_only_summary_deprecated.__doc__ == (
            "My docstring.\n"
            "\n"
            ".. deprecated:: 1.2.3\n"
            "    This is deprecated!!\n"
        )

    @pytest.mark.unit
    def test_deprecated_decorator_no_docstring(self):
        assert _func_no_docstring_deprecated.__doc__ == (
            "\n" "\n" ".. deprecated:: 1.2.3\n" "    This is deprecated!!\n"
        )


# @new_method_or_class


@new_method_or_class(version="1.2.3", message="Added in version 1.2.3")
def _func_full_docstring_new_method(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@new_method_or_class(version="1.2.3")
def _func_full_docstring_new_method_no_message(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@new_method_or_class(version="1.2.3", message="Added in version 1.2.3")
def _func_only_summary_new_method(some_arg, other_arg):
    """My docstring."""
    pass


@new_method_or_class(version="1.2.3", message="Added in version 1.2.3")
def _func_no_docstring_new_method(some_arg, other_arg):
    pass


class TestNewMethod:
    @pytest.mark.unit
    def test_new_method_decorator_full_docstring(self):
        assert _func_full_docstring_new_method.__doc__ == (
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

    @pytest.mark.unit
    def test_new_method_decorator_full_docstring_no_message(self):
        assert _func_full_docstring_new_method_no_message.__doc__ == (
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

    @pytest.mark.unit
    def test_new_method_decorator_only_summary(self):
        assert _func_only_summary_new_method.__doc__ == (
            "My docstring.\n"
            "\n"
            ".. versionadded:: 1.2.3\n"
            "    Added in version 1.2.3\n"
        )

    @pytest.mark.unit
    def test_new_method_decorator_no_docstring(self):
        assert _func_no_docstring_new_method.__doc__ == (
            "\n" "\n" ".. versionadded:: 1.2.3\n" "    Added in version 1.2.3\n"
        )


# All Method Level Decorators


@public_api
@new_method_or_class(version="1.2.3", message="Added in version 1.2.3")
@deprecated_method_or_class(version="1.2.3", message="This is deprecated!!")
def _func_full_docstring_all_method_decorators(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@pytest.mark.unit
def test_all_method_decorators_full_docstring():
    assert _func_full_docstring_all_method_decorators.__doc__ == (
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


@pytest.mark.unit
def test_method_decorators_do_not_change_function_name():
    assert (
        _func_full_docstring_all_method_decorators.__name__
        == "_func_full_docstring_all_method_decorators"
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


@deprecated_argument(argument_name="some_arg", version="1.2.3", message="some msg")
def _func_full_docstring_deprecated_argument_no_description(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg:
        other_arg: describe other_arg
    """
    pass


class TestDeprecatedArgument:
    @pytest.mark.unit
    def test_deprecated_decorator_full_docstring_deprecated_argument(self):
        assert _func_full_docstring_deprecated_argument.__doc__ == (
            "My docstring.\n"
            "\n"
            "Longer description.\n"
            "\n"
            "Args:\n"
            "    some_arg:\n"
            "        describe some_arg\n"
            "        \n"
            "        .. deprecated:: 1.2.3\n"
            "            some msg\n"
            "    other_arg:\n"
            "        describe other_arg"
        )
        assert (
            _func_full_docstring_deprecated_argument.__name__
            == "_func_full_docstring_deprecated_argument"
        )

    @pytest.mark.unit
    def test_deprecated_decorator_full_docstring_deprecated_argument_no_description(
        self,
    ):
        assert _func_full_docstring_deprecated_argument_no_description.__doc__ == (
            "My docstring.\n"
            "\n"
            "Longer description.\n"
            "\n"
            "Args:\n"
            "    some_arg:\n"
            "        \n"
            "        \n"
            "        .. deprecated:: 1.2.3\n"
            "            some msg\n"
            "    other_arg:\n"
            "        describe other_arg"
        )

    @pytest.mark.unit
    def test_deprecated_decorator_full_docstring_deprecated_argument_missing(self):
        with pytest.raises(ValueError) as e:

            @deprecated_argument(
                argument_name="this_arg_doesnt_exist",
                version="1.2.3",
                message="some msg",
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


@new_argument(argument_name="some_arg", version="1.2.3", message="some msg")
@new_argument(argument_name="other_arg", version="1.2.3", message="some other msg")
def _func_full_docstring_two_new_arguments(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg: describe some_arg
        other_arg: describe other_arg
    """
    pass


@new_argument(argument_name="some_arg", version="1.2.3", message="some msg")
def _func_full_docstring_new_argument_no_description(some_arg, other_arg):
    """My docstring.

    Longer description.

    Args:
        some_arg:
        other_arg: describe other_arg
    """
    pass


class TestNewArgument:
    @pytest.mark.unit
    def test_new_argument_decorator_full_docstring_new_argument(self):
        assert _func_full_docstring_new_argument.__doc__ == (
            "My docstring.\n"
            "\n"
            "Longer description.\n"
            "\n"
            "Args:\n"
            "    some_arg:\n"
            "        describe some_arg\n"
            "        \n"
            "        .. versionadded:: 1.2.3\n"
            "            some msg\n"
            "    other_arg:\n"
            "        describe other_arg"
        )

    @pytest.mark.unit
    def test_new_argument_decorator_full_docstring_two_new_arguments(self):
        assert _func_full_docstring_two_new_arguments.__doc__ == (
            "My docstring.\n"
            "\n"
            "Longer description.\n"
            "\n"
            "Args:\n"
            "    some_arg:\n"
            "        describe some_arg\n"
            "        \n"
            "        .. versionadded:: 1.2.3\n"
            "            some msg\n"
            "    other_arg:\n"
            "        describe other_arg\n"
            "        \n"
            "        .. versionadded:: 1.2.3\n"
            "            some other msg"
        )

    @pytest.mark.unit
    def test_new_argument_full_docstring_new_argument_no_description(self):
        assert _func_full_docstring_new_argument_no_description.__doc__ == (
            "My docstring.\n"
            "\n"
            "Longer description.\n"
            "\n"
            "Args:\n"
            "    some_arg:\n"
            "        \n"
            "        \n"
            "        .. versionadded:: 1.2.3\n"
            "            some msg\n"
            "    other_arg:\n"
            "        describe other_arg"
        )

    @pytest.mark.unit
    def test_new_argument_full_docstring_new_argument_missing(self):
        with pytest.raises(ValueError) as e:

            @new_argument(
                argument_name="this_arg_doesnt_exist",
                version="1.2.3",
                message="some msg",
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
@new_method_or_class(version="1.2.3", message="Added in version 1.2.3")
@deprecated_method_or_class(version="1.2.3", message="This is deprecated!!")
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


@pytest.mark.unit
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
        "    some_arg:\n"
        "        describe some_arg\n"
        "\n"
        "        .. versionadded:: 1.2.3\n"
        "            some msg\n"
        "    other_arg:\n"
        "        describe other_arg\n"
        "\n"
        "        .. deprecated:: 1.2.3\n"
        "            some msg"
    )


@public_api
@new_method_or_class(version="1.2.3", message="Added in version 1.2.3")
@deprecated_method_or_class(version="1.2.3", message="This is deprecated!!")
@new_argument(argument_name="some_arg", version="1.2.3", message="some msg")
@deprecated_argument(argument_name="other_arg", version="1.2.3", message="some msg")
def _func_full_docstring_all_decorators_all_sections(
    arg_not_decorated, some_arg, other_arg, other_arg_not_decorated
):
    """My docstring.

    Longer description.

    Usage:
        Some usage example.

    Args:
        arg_not_decorated: description.
        some_arg: describe some_arg.
        other_arg: describe other_arg.
        other_arg_not_decorated: description.

    Returns:
        Some returns value.
        Next line.

    Raises:
        SomeError: Some error text.
    """
    pass


@pytest.mark.unit
def test_all_decorators_full_docstring_all_sections():
    """Makes sure that Returns and Raises are rendered correctly in the context of a full docstring."""

    assert _func_full_docstring_all_decorators_all_sections.__doc__ == (
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
        "Usage:\n"
        "    Some usage example.\n"
        "\n"
        "Args:\n"
        "    arg_not_decorated:\n"
        "        description.\n"
        "    some_arg:\n"
        "        describe some_arg.\n"
        "\n"
        "        .. versionadded:: 1.2.3\n"
        "            some msg\n"
        "    other_arg:\n"
        "        describe other_arg.\n"
        "\n"
        "        .. deprecated:: 1.2.3\n"
        "            some msg\n"
        "    other_arg_not_decorated:\n"
        "        description.\n"
        "\n"
        "Returns:\n"
        "    :\n"
        "        Some returns value.\n"
        "        Next line.\n"
        "\n"
        "Raises:\n"
        "    SomeError:\n"
        "        Some error text."
    )


@pytest.mark.unit
def test_all_decorators_do_not_change_function_name():
    assert (
        _func_full_docstring_all_decorators.__name__
        == "_func_full_docstring_all_decorators"
    )


# Class level decorators


@public_api
class _ClassFullDocstringPublicAPI:
    """Docstring summary.

    Longer description.

    Args:
        some_arg: some_arg description.
        other_arg: other_arg description.
    """

    def __init__(self, some_arg, other_arg) -> None:
        self.some_arg = some_arg
        self.other_arg = other_arg


@deprecated_method_or_class(version="1.2.3", message="This is deprecated!!")
@new_method_or_class(version="1.2.3", message="Added in version 1.2.3")
class _ClassFullDocstringDeprecatedAndNewAtClassLevel:
    """Docstring summary.

    Longer description.

    Args:
        some_arg: some_arg description.
        other_arg: other_arg description.
    """

    def __init__(self, some_arg, other_arg) -> None:
        self.some_arg = some_arg
        self.other_arg = other_arg


@deprecated_argument(
    argument_name="some_arg", version="1.2.3", message="This is deprecated!!"
)
@new_argument(
    argument_name="other_arg", version="1.2.3", message="Added in version 1.2.3"
)
class _ClassFullDocstringDeprecatedAndNewAtArgumentLevel:
    """Docstring summary.

    Longer description.

    Args:
        some_arg: some_arg description.
        other_arg: other_arg description.
    """

    def __init__(self, some_arg, other_arg) -> None:
        self.some_arg = some_arg
        self.other_arg = other_arg


class TestClassDocstringDecorators:
    @pytest.mark.unit
    def test_class_full_docstring_public_api(self):
        assert _ClassFullDocstringPublicAPI.__doc__ == (
            "--Public API--Docstring summary.\n"
            "\n"
            "    Longer description.\n"
            "\n"
            "    Args:\n"
            "        some_arg: some_arg description.\n"
            "        other_arg: other_arg description.\n"
            "    "
        )
        assert _ClassFullDocstringPublicAPI.__name__ == "_ClassFullDocstringPublicAPI"

    @pytest.mark.unit
    def test_class_full_docstring_deprecated_and_new_class_level(self):
        assert _ClassFullDocstringDeprecatedAndNewAtClassLevel.__doc__ == (
            "Docstring summary.\n"
            "\n"
            ".. deprecated:: 1.2.3\n"
            "    This is deprecated!!\n"
            "\n"
            "\n"
            ".. versionadded:: 1.2.3\n"
            "    Added in version 1.2.3\n"
            "\n"
            "\n"
            "Longer description.\n"
            "\n"
            "Args:\n"
            "    some_arg: some_arg description.\n"
            "    other_arg: other_arg description.\n"
        )
        assert (
            _ClassFullDocstringDeprecatedAndNewAtClassLevel.__name__
            == "_ClassFullDocstringDeprecatedAndNewAtClassLevel"
        )

    @pytest.mark.unit
    def test_class_full_docstring_deprecated_and_new_argument_level(self):
        assert _ClassFullDocstringDeprecatedAndNewAtArgumentLevel.__doc__ == (
            "Docstring summary.\n"
            "\n"
            "Longer description.\n"
            "\n"
            "Args:\n"
            "    some_arg:\n"
            "        some_arg description.\n"
            "        \n"
            "        .. deprecated:: 1.2.3\n"
            "            This is deprecated!!\n"
            "    other_arg:\n"
            "        other_arg description.\n"
            "        \n"
            "        .. versionadded:: 1.2.3\n"
            "            Added in version 1.2.3"
        )
        assert (
            _ClassFullDocstringDeprecatedAndNewAtArgumentLevel.__name__
            == "_ClassFullDocstringDeprecatedAndNewAtArgumentLevel"
        )
