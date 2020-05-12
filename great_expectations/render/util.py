# -*- coding: utf-8 -*-

"""Rendering utility"""
import decimal
import locale

DEFAULT_PRECISION = 4
# create a new context for this task
ctx = decimal.Context()
# Lowering precision from the system default (28) can allow additional control over display
ctx.prec = DEFAULT_PRECISION


def num_to_str(f, precision=DEFAULT_PRECISION, use_locale=False, no_scientific=False):
    """Convert the given float to a string, centralizing standards for precision and decisions about scientific
    notation. Adds an approximately equal sign in the event precision loss (e.g. rounding) has occurred.

    There's a good discussion of related issues here:
        https://stackoverflow.com/questions/38847690/convert-float-to-string-in-positional-format-without-scientific-notation-and-fa

    Args:
        f: the number to format
        precision: the number of digits of precision to display
        use_locale: if True, use locale-specific formatting (e.g. adding thousands separators)
        no_scientific: if True, print all available digits of precision without scientific notation. This may insert
            leading zeros before very small numbers, causing the resulting string to be longer than `precision`
            characters

    Returns:
        A string representation of the float, according to the desired parameters

    """
    assert not (use_locale and no_scientific)
    if precision != DEFAULT_PRECISION:
        local_context = decimal.Context()
        local_context.prec = precision
    else:
        local_context = ctx
    # We cast to string; we want to avoid precision issues, but format everything as though it were a float.
    # So, if it's not already a float, we will append a decimal point to the string representation
    s = repr(f)
    if not isinstance(f, float):
        s += locale.localeconv().get("decimal_point") + "0"
    d = local_context.create_decimal(s)
    if no_scientific:
        result = format(d, "f")
    elif use_locale:
        result = format(d, "n")
    else:
        result = format(d, "g")
    if f != locale.atof(result):
        # result = '≈' + result
        #  ≈  # \u2248
        result = "≈" + result
    if "e" not in result and "E" not in result:
        result = result.rstrip("0").rstrip(locale.localeconv().get("decimal_point"))
    return result


SUFFIXES = {1: "st", 2: "nd", 3: "rd"}


def ordinal(num):
    """Convert a number to ordinal"""
    # Taken from https://codereview.stackexchange.com/questions/41298/producing-ordinal-numbers/41301
    # Consider a library like num2word when internationalization comes
    if 10 <= num % 100 <= 20:
        suffix = "th"
    else:
        # the second parameter is a default.
        suffix = SUFFIXES.get(num % 10, "th")
    return str(num) + suffix
