import six
import re

try:
    from termcolor import colored
except ImportError:
    colored = None


def cli_message(string):
    mod_string = re.sub(
        "<blue>(.*?)</blue>",
        colored("\g<1>", "blue"),
        string
    )

    six.print_(colored(mod_string))
