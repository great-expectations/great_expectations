import sys

from subprocess import (
    CalledProcessError,
    STDOUT,
    check_output
)

import traceback


def execute_shell_command(command: str, *, cwd: str = None, env: dict = None):
    # TODO progress bar
    # TODO replace prints with logging
    print(f"\n\nrunning execute_shell_command for {command}")
    returncode: int = 0
    try:
        sh_out: str = check_output(['bash', '-c', command], cwd=cwd, env=env, shell=False, stderr=STDOUT, universal_newlines=True)
        sh_out = sh_out.strip()
        print(sh_out)
    except CalledProcessError as cpe:
        returncode = cpe.returncode
        sys.stderr.write(cpe.output)
        sys.stderr.flush()
        exception_message: str = "A Sub-Process call Exception occurred.\n"
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(cpe).__name__}: "{str(cpe)}".  Traceback: "{exception_traceback}".'
        print(exception_message)

    return returncode
