"""Regression tests for filesystem-backed reads/writes under a non-UTF-8 locale.

See https://github.com/fivetran/great_expectations/issues/12120. GX always writes
its own files as UTF-8, but several read paths opened files with a bare open(),
which resolves its encoding from the process's ambient locale. On a host whose
locale encoding isn't UTF-8 (chiefly Windows, where nothing coerces the locale
to UTF-8 the way POSIX's PEP 538 does), reading back a value GX itself wrote can
raise UnicodeDecodeError.

These tests force a non-UTF-8 locale the same way the issue's own repro does:
by spawning a subprocess with LC_ALL, LANG, PYTHONCOERCECLOCALE, and PYTHONUTF8
set so that open()'s default encoding resolves to something other than UTF-8.
A test that instead relies on the ambient locale would pass either way here,
since CI runners are UTF-8 and no workflow sets these variables.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import pathlib

NON_UTF8_ENV = dict(
    os.environ,
    LC_ALL="C",
    LANG="C",
    PYTHONCOERCECLOCALE="0",
    PYTHONUTF8="0",
)

NON_ASCII_VALUE = "Prüfung ünïcödé Straße café naïve 中文测试"


def _run_under_non_utf8_locale(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # FIXME CoP
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=NON_UTF8_ENV,
        check=False,
    )


@pytest.mark.filesystem
def test_tuple_filesystem_store_backend_reads_own_writes_under_non_utf8_locale(
    tmp_path: pathlib.Path,
) -> None:
    """A value TupleFilesystemStoreBackend writes must read back unchanged, regardless
    of what encoding the process's ambient locale resolves to.
    """  # FIXME CoP
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    payload_path = tmp_path / "payload.txt"
    payload_path.write_text(NON_ASCII_VALUE, encoding="utf-8")

    script = textwrap.dedent(f"""
        from great_expectations.data_context.store.tuple_store_backend import (
            TupleFilesystemStoreBackend,
        )

        import os

        assert open(os.devnull).encoding != "utf-8"  # locale override did not take effect

        with open({str(payload_path)!r}, encoding="utf-8") as f:
            non_ascii_value = f.read()

        backend = TupleFilesystemStoreBackend(
            root_directory={str(store_dir)!r},
            base_directory={str(store_dir)!r},
            filepath_template="my_file_{{0}}",
        )
        backend.set(("AAA",), non_ascii_value)
        assert backend.get(("AAA",)) == non_ascii_value
        print("OK")
    """)

    result = _run_under_non_utf8_locale(script)

    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


@pytest.mark.filesystem
def test_file_data_context_reloads_non_ascii_project_yaml_under_non_utf8_locale(
    tmp_path: pathlib.Path,
) -> None:
    """A great_expectations.yml written on one host (or under one locale) must load on
    another, even if a data source name or other config value contains non-ASCII text.

    Both the write and the reload run under a forced non-UTF-8 locale: a write under the
    ambient (UTF-8) locale would emit correct bytes regardless of whether the write path
    pins its encoding, leaving nothing for the reload assertion to catch.
    """  # FIXME CoP
    project_root = tmp_path / "project"
    payload_path = tmp_path / "payload.txt"
    payload_path.write_text(NON_ASCII_VALUE, encoding="utf-8")

    write_script = textwrap.dedent(f"""
        import great_expectations as gx

        import os

        assert open(os.devnull).encoding != "utf-8"  # locale override did not take effect

        with open({str(payload_path)!r}, encoding="utf-8") as f:
            non_ascii_value = f.read()

        context = gx.get_context(mode="file", context_root_dir={str(project_root)!r})
        context.data_sources.add_pandas(name=non_ascii_value)
    """)
    write_result = _run_under_non_utf8_locale(write_script)
    assert write_result.returncode == 0, write_result.stderr

    reload_script = textwrap.dedent(f"""
        import great_expectations as gx

        import os

        assert open(os.devnull).encoding != "utf-8"  # locale override did not take effect

        with open({str(payload_path)!r}, encoding="utf-8") as f:
            non_ascii_value = f.read()

        context = gx.get_context(mode="file", context_root_dir={str(project_root)!r})
        assert non_ascii_value in context.data_sources.all()
        print("OK")
    """)

    reload_result = _run_under_non_utf8_locale(reload_script)

    assert reload_result.returncode == 0, reload_result.stderr
    assert "OK" in reload_result.stdout


@pytest.mark.filesystem
def test_inline_store_backend_saves_non_ascii_variable_under_non_utf8_locale(
    tmp_path: pathlib.Path,
) -> None:
    """InlineStoreBackend._save_changes() is a separate write path from
    FileDataContext._save_project_config(): it backs DataContextVariables (things like
    config_variables_file_path), not fluent datasources, which persist through
    _save_project_config's own to_yaml call instead. Exercise it directly by setting a
    variable to a non-ASCII value and saving.
    """  # FIXME CoP
    project_root = tmp_path / "project"
    payload_path = tmp_path / "payload.txt"
    payload_path.write_text(NON_ASCII_VALUE, encoding="utf-8")

    write_script = textwrap.dedent(f"""
        import great_expectations as gx

        import os

        assert open(os.devnull).encoding != "utf-8"  # locale override did not take effect

        with open({str(payload_path)!r}, encoding="utf-8") as f:
            non_ascii_value = f.read()

        context = gx.get_context(mode="file", context_root_dir={str(project_root)!r})
        context.variables.config_variables_file_path = non_ascii_value
        context.variables.save()
    """)
    write_result = _run_under_non_utf8_locale(write_script)
    assert write_result.returncode == 0, write_result.stderr

    reload_script = textwrap.dedent(f"""
        import great_expectations as gx

        import os

        assert open(os.devnull).encoding != "utf-8"  # locale override did not take effect

        with open({str(payload_path)!r}, encoding="utf-8") as f:
            non_ascii_value = f.read()

        context = gx.get_context(mode="file", context_root_dir={str(project_root)!r})
        assert context.variables.config_variables_file_path == non_ascii_value
        print("OK")
    """)

    reload_result = _run_under_non_utf8_locale(reload_script)

    assert reload_result.returncode == 0, reload_result.stderr
    assert "OK" in reload_result.stdout
