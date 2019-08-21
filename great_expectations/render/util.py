"""Rendering utility"""

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
