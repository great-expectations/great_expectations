import pathlib
from typing import Callable

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
from great_expectations.data_context.data_context.serializable_data_context import (
    SerializableDataContext,
)
from great_expectations.data_context.migrator.file_migrator import FileMigrator
from great_expectations.data_context.types.base import (
    DataContextConfigDefaults,
    DatasourceConfig,
)
from tests.test_utils import working_directory


@pytest.fixture
def file_migrator(
    construct_file_migrator: Callable,
    ephemeral_context_with_defaults: EphemeralDataContext,
) -> FileMigrator:
    return construct_file_migrator(ephemeral_context_with_defaults)


@pytest.fixture
def construct_file_migrator() -> Callable:
    def _construct_file_migrator(context: AbstractDataContext):
        return FileMigrator(
            primary_stores=context.stores,
            datasource_store=context._datasource_store,
            variables=context.variables,
            fluent_config=context.fluent_config,
        )

    return _construct_file_migrator


@pytest.mark.integration
def test_migrate_scaffolds_filesystem(
    tmp_path: pathlib.Path, file_migrator: FileMigrator
):
    # Construct and run migrator
    tmp_path.mkdir(exist_ok=True)
    with working_directory(str(tmp_path)):
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
def test_migrate_transfers_store_contents(
    tmp_path: pathlib.Path,
    construct_file_migrator: Callable,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    # Test setup
    context = ephemeral_context_with_defaults

    suite_names = ["suite_a", "suite_b", "suite_c"]
    for name in suite_names:
        context.add_expectation_suite(expectation_suite_name=name)

    # Construct and run migrator
    tmp_path.mkdir(exist_ok=True)
    with working_directory(str(tmp_path)):
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
def test_migrate_transfers_datasources(
    tmp_path: pathlib.Path,
    construct_file_migrator: Callable,
    ephemeral_context_with_defaults: EphemeralDataContext,
    block_config_datasource_config: DatasourceConfig,
):
    # Test setup
    context = ephemeral_context_with_defaults
    datasource_name = "my_datasource_awaiting_migration"

    config_dict = block_config_datasource_config.to_dict()
    for attr in ("class_name", "module_name"):
        config_dict.pop(attr)
    config_dict["name"] = datasource_name

    context.add_datasource(**config_dict)

    # Construct and run migrator
    tmp_path.mkdir(exist_ok=True)
    with working_directory(str(tmp_path)):
        file_migrator = construct_file_migrator(context)
        migrated_context = file_migrator.migrate()

    # # Check proper config updates
    assert datasource_name in migrated_context.datasources

    # Check proper filesystem interaction
    root = pathlib.Path(migrated_context.root_directory)
    project_config = root.joinpath(SerializableDataContext.GX_YML)

    with project_config.open() as f:
        contents = f.read()

    assert datasource_name in contents


@pytest.mark.integration
def test_migrate_transfers_fluent_datasources(
    tmp_path: pathlib.Path,
    construct_file_migrator: Callable,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    # Test setup
    context = ephemeral_context_with_defaults
    datasource_name = "my_experimental_datasource_awaiting_migration"

    context.sources.add_pandas(datasource_name)
    context._synchronize_fluent_datasources()

    # Construct and run migrator
    tmp_path.mkdir(exist_ok=True)
    with working_directory(str(tmp_path)):
        file_migrator = construct_file_migrator(context)
        migrated_context = file_migrator.migrate()

    # # Check proper config updates
    assert datasource_name in migrated_context.datasources

    # Check proper filesystem interaction
    root = pathlib.Path(migrated_context.root_directory)
    project_config = root.joinpath(SerializableDataContext.GX_YML)

    with project_config.open() as f:
        contents = f.read()

    assert datasource_name in contents


@pytest.mark.integration
def test_migrate_transfers_doc_sites(
    tmp_path: pathlib.Path,
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
    tmp_path.mkdir(exist_ok=True)
    with working_directory(str(tmp_path)):
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
