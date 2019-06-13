import six

from pyfiglet import figlet_format

try:
    from termcolor import colored
except ImportError:
    colored = None


def cli_message(string, color, font="big", figlet=False):
    if colored:
        if not figlet:
            six.print_(colored(string, color))
        else:
            six.print_(colored(figlet_format(
                string, font=font), color))
    else:
        six.print_(string)
