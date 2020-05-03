# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import os

import mock
from great_expectations.cli import cli

@mock.patch.dict(os.environ, {'GE_JPYTER_CMD': None})
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_launch_jupyter_notebook_env_none(mock_env, mock_subprocess):
    cli.toolkit.test_launch_jupyter_notebook('test_path')
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", "test_path"])


@mock.patch.dict(os.environ, {'GE_JPYTER_CMD': 'jupyter notebook --test-args'})
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_launch_jupyter_notebook_env_none(mock_env, mock_subprocess):
    cli.toolkit.test_launch_jupyter_notebook('test_path')
    mock_subprocess.assert_called_once_with(["jupyter notebook --test-args test_path"])






