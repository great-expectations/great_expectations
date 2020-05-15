#!/usr/bin/env python3

import os
import sys
import datetime

from subprocess import (
    CalledProcessError,
    STDOUT,
    check_output
)

import traceback


def execute_shell_command(command: str, *, cwd: str = None, env: dict = None):
    returncode: int = 0
    try:
        sh_out: str = check_output(['bash', '-c', command], cwd=cwd, env=env, stderr=STDOUT, universal_newlines=True)
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
        #raise cpe

    return returncode


cwd: str = os.getcwd()
#print(cwd)
path_env_var: str = os.pathsep.join([os.environ.get('PATH', os.defpath), cwd])
env: dict = dict(os.environ, PATH=path_env_var)
#print(path_env_var)
#print(env)

#command: str = '/usr/bin/which virtualenv'
#command: str = '/usr/bin/which python'
#command: str = '/usr/bin/which pip'
#command: str = 'pip install sqlalchemy'
#command: str = 'pip install great_expectations'
command: str = 'pip freeze'

returncode = execute_shell_command(command=command, cwd=cwd, env=env)
print(returncode)
