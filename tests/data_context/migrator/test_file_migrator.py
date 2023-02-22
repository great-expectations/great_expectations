import pathlib
from typing import Callable

import py
import pytest

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.migrator.file_migrator import FileMigrator
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    InMemoryStoreBackendDefaults,
)
from tests.test_utils import working_directory


@pytest.fixture
def ephemeral_context_with_defaults() -> EphemeralDataContext:
    project_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(init_temp_docs_sites=True)
    )
    return EphemeralDataContext(project_config=project_config)


@pytest.fixture
def construct_file_migrator() -> Callable:
    def _construct_file_migrator(context: AbstractDataContext):
        return FileMigrator(
            primary_stores=context.stores,
            datasource_store=context._datasource_store,
            variables=context.variables,
        )

    return _construct_file_migrator


@pytest.fixture
def file_migrator(
    construct_file_migrator: Callable,
    ephemeral_context_with_defaults: EphemeralDataContext,
) -> FileMigrator:
    return construct_file_migrator(ephemeral_context_with_defaults)


@pytest.mark.integration
def test_migrate_scaffolds_filesystem(
    tmpdir: py.path.local, file_migrator: FileMigrator
):
    # Construct and run migrator
    d = tmpdir.mkdir("tmp")
    with working_directory(str(d)):
        migrated_context = file_migrator.migrate()

    assert isinstance(migrated_context, FileDataContext)

    # Check proper filesystem interaction
    root = pathlib.Path(migrated_context.root_directory)
    contents = sorted(str(f.stem) for f in root.glob("*") if f.is_dir())
    assert contents == [
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.EXPECTATIONS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PLUGINS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PROFILERS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.UNCOMMITTED.value,
    ]


@pytest.mark.integration
def test_migrate_transfers_persisted_objects(
    tmpdir: py.path.local,
    construct_file_migrator: Callable,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    # Test setup
    context = ephemeral_context_with_defaults

    suite_names = ["suite_a", "suite_b", "suite_c"]
    for name in suite_names:
        context.add_expectation_suite(expectation_suite_name=name)

    # Construct and run migrator
    d = tmpdir.mkdir("tmp")
    with working_directory(str(d)):
        file_migrator = construct_file_migrator(context)
        migrated_context = file_migrator.migrate()

    # Check proper config updates
    assert sorted(migrated_context.list_expectation_suite_names()) == suite_names

    # Check proper filesystem interaction
    root = pathlib.Path(migrated_context.root_directory)
    expectations_dir = root.joinpath(
        DataContextConfigDefaults.EXPECTATIONS_BASE_DIRECTORY.value
    )
    contents = sorted(str(f.stem) for f in expectations_dir.glob("*.json"))

    assert contents == suite_names


@pytest.mark.integration
def test_migrate_transfers_doc_sites(
    tmpdir: py.path.local,
    construct_file_migrator: Callable,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    # Test setup
    context = ephemeral_context_with_defaults

    site_configs = context.variables.data_docs_sites or {}
    default_site_name = DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITE_NAME.value
    assert len(site_configs) == 1 and default_site_name in site_configs

    site_names = ["site_a", "site_b"]
    for name in site_names:
        site_configs[name] = site_configs[default_site_name]

    context.variables.data_docs_sites = site_configs
    context.build_data_docs()

    # Construct and run migrator
    d = tmpdir.mkdir("tmp")
    with working_directory(str(d)):
        file_migrator = construct_file_migrator(context)
        migrated_context = file_migrator.migrate()

    # Check proper config updates
    expected_site_names = [default_site_name] + site_names
    actual_site_names = sorted(
        site_url["site_name"] for site_url in migrated_context.get_docs_sites_urls()
    )

    assert actual_site_names == expected_site_names

    # Check proper filesystem interaction
    root = pathlib.Path(migrated_context.root_directory)
    docs_sites_dir = root.joinpath(
        DataContextConfigDefaults.DEFAULT_DATA_DOCS_BASE_DIRECTORY_RELATIVE_NAME.value
    )
    contents = sorted(str(f.stem) for f in docs_sites_dir.glob("*"))

    assert contents == expected_site_names
