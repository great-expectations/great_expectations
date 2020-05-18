import os
import sys
import traceback
from subprocess import STDOUT, CalledProcessError, check_output

from great_expectations.core import logger


def execute_shell_command(command: str) -> int:
    """
    Execute a shell (bash in the present case) command from inside Python program.

    While developed independently, this function is very similar to the one, offered in this StackOverflow article:
    https://stackoverflow.com/questions/30993411/environment-variables-using-subprocess-check-output-python

    :param command: bash command -- as if typed in a shell/Terminal window
    :return: status code -- 0 if successful; all other values (1 is the most common) indicate an error
    """
    cwd: str = os.getcwd()

    path_env_var: str = os.pathsep.join([os.environ.get("PATH", os.defpath), cwd])
    env: dict = dict(os.environ, PATH=path_env_var)

    status_code: int = 0
    try:
        sh_out: str = check_output(
            ["bash", "-c", command],
            cwd=cwd,
            env=env,
            shell=False,
            stderr=STDOUT,
            universal_newlines=True,
        )
        sh_out = sh_out.strip()
        logger.info(sh_out)
    except CalledProcessError as cpe:
        status_code = cpe.returncode
        sys.stderr.write(cpe.output)
        sys.stderr.flush()
        exception_message: str = "A Sub-Process call Exception occurred.\n"
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(cpe).__name__}: "{str(cpe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)

    return status_code
