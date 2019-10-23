"""Rendering utility"""
import decimal
import locale
import re
locale.setlocale(locale.LC_ALL, '')


DEFAULT_PRECISION = 4
# create a new context for this task
ctx = decimal.Context()
# Lowering precision from the system default (28) can allow additional control over display
ctx.prec = DEFAULT_PRECISION


def float_to_str(f, precision=DEFAULT_PRECISION, use_locale=False, no_scientific=False):
    """Convert the given float to a string, centralizing standards for precision and decisions about scientific notation.

    There's a good discussion of related issues here:
        https://stackoverflow.com/questions/38847690/convert-float-to-string-in-positional-format-without-scientific-notation-and-fa

    Args:
        f: the number to format
        format_code: the code to use as part of the format string. Use 'n' for locale-specific, 'f' for floating point
            (NOT scientific notation), or 'g' for general floating point (MAY be scientific notation).
            We recommend that you do NOT use '%' and instead pre-scale any values, since that adds a percent sign and so
            can conflict with other desired string styling.
        precision: the number of digits of precision to display

    Returns:
        A string representation of the float, according to the desired parameters

    """
    assert not (use_locale and no_scientific)
    if use_locale:
        rounded = round(f, precision)
        result = format(rounded, "n")
    elif no_scientific:
        rounded = round(f, precision)
        if precision != DEFAULT_PRECISION:
            local_context = decimal.Context()
            local_context.prec = precision
        else:
            local_context = ctx

        d = local_context.create_decimal(repr(rounded))
        result = format(d, 'f')
    else:
        result = format(f, "." + str(precision) + "g")
        rounded = float(result)
    if rounded != f:
        result = '~' + result
    return result


SUFFIXES = {1: 'st', 2: 'nd', 3: 'rd'}


def ordinal(num):
    """Convert a number to ordinal"""
    # Taken from https://codereview.stackexchange.com/questions/41298/producing-ordinal-numbers/41301
    # Consider a library like num2word when internationalization comes
    if 10 <= num % 100 <= 20:
        suffix = 'th'
    else:
        # the second parameter is a default.
        suffix = SUFFIXES.get(num % 10, 'th')
    return str(num) + suffix
