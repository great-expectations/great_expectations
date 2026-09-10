import errno
import json
import os
import uuid
from typing import Optional
from unittest import mock

import pytest

from great_expectations.compatibility.pyparsing import Word, hexnums
from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store import (
    InMemoryStoreBackend,
    StoreBackend,
    TupleFilesystemStoreBackend,
)
from great_expectations.data_context.store.inline_store_backend import (
    InlineStoreBackend,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import InvalidKeyError, StoreBackendError, StoreError
from great_expectations.self_check.util import expectationSuiteSchema
from great_expectations.util import (
    gen_directory_tree_str,
)
from tests import test_utils

yaml = YAMLHandler()


@pytest.fixture()
def parameterized_expectation_suite(empty_data_context_stats_enabled):
    context = empty_data_context_stats_enabled
    fixture_path = file_relative_path(
        __file__,
        "../../test_fixtures/expectation_suites/parameterized_expression_expectation_suite_fixture.json",
    )
    with open(
        fixture_path,
    ) as suite:
        expectation_suite_dict: dict = expectationSuiteSchema.load(json.load(suite))
        return ExpectationSuite(**expectation_suite_dict, data_context=context)


@pytest.mark.unit
def test_StoreBackendValidation():
    backend = InMemoryStoreBackend()

    backend._validate_key(("I", "am", "a", "string", "tuple"))

    with pytest.raises(TypeError):
        backend._validate_key("nope")

    with pytest.raises(TypeError):
        backend._validate_key(("I", "am", "a", "string", 100))

    with pytest.raises(TypeError):
        backend._validate_key(("I", "am", "a", "string", None))

    # zero-length tuple is allowed
    backend._validate_key(())


def check_store_backend_store_backend_id_functionality(
    store_backend: StoreBackend, store_backend_id: Optional[str] = None
) -> None:
    """
    Assertions to check if a store backend is handling reading and writing a store_backend_id appropriately.
    Args:
        store_backend: Instance of subclass of StoreBackend to test e.g. TupleFilesystemStoreBackend
        store_backend_id: Manually input store_backend_id
    Returns:
        None
    """  # noqa: E501 # FIXME CoP
    # Check that store_backend_id exists can be read
    assert store_backend.store_backend_id is not None
    store_error_uuid = "00000000-0000-0000-0000-00000000e003"
    assert store_backend.store_backend_id != store_error_uuid
    if store_backend_id:
        assert store_backend.store_backend_id == store_backend_id
    # Check that store_backend_id is a valid UUID
    assert isinstance(store_backend.store_backend_id, uuid.UUID)
    # Check in file stores that the actual file exists
    assert store_backend.has_key(key=(".ge_store_backend_id",))

    # Check file stores for the file in the correct format
    store_backend_id_from_file = store_backend.get(key=(".ge_store_backend_id",))
    store_backend_id_file_parser = "store_backend_id = " + Word(hexnums + "-")
    parsed_store_backend_id = store_backend_id_file_parser.parse_string(store_backend_id_from_file)
    assert test_utils.validate_uuid4(parsed_store_backend_id[1])


@pytest.mark.filesystem
def test_StoreBackend_id_initialization(tmp_path_factory):
    """
    What does this test and why?

    A StoreBackend should have a store_backend_id property. That store_backend_id should be read and initialized
    from an existing persistent store_backend_id during instantiation, or a new store_backend_id should be generated
    and persisted. The store_backend_id should be a valid UUIDv4
    If a new store_backend_id cannot be persisted, use an ephemeral store_backend_id.
    Persistence should be in a .ge_store_backend_id file for for filesystem and blob-stores.

    Note: StoreBackend & TupleStoreBackend are abstract classes, so we will test the
    concrete classes that inherit from them.
    See also test_database_store_backend::test_database_store_backend_id_initialization
    """  # noqa: E501 # FIXME CoP

    # InMemoryStoreBackend
    # Initialize without store_backend_id and check that it is generated correctly
    in_memory_store_backend = InMemoryStoreBackend()
    check_store_backend_store_backend_id_functionality(store_backend=in_memory_store_backend)

    # Create a new store with the same config and make sure it reports the same store_backend_id
    # in_memory_store_backend_duplicate = InMemoryStoreBackend()
    # assert in_memory_store_backend.store_backend_id == in_memory_store_backend_duplicate.store_backend_id  # noqa: E501 # FIXME CoP
    # This is not currently implemented for the InMemoryStoreBackend, the store_backend_id is ephemeral since  # noqa: E501 # FIXME CoP
    # there is no place to persist it.

    # TupleFilesystemStoreBackend
    # Initialize without store_backend_id and check that it is generated correctly
    path = "dummy_str"
    full_test_dir = tmp_path_factory.mktemp("test_StoreBackend_id_initialization__dir")
    test_dir = full_test_dir.parts[-1]
    project_path = str(full_test_dir)

    tuple_filesystem_store_backend = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
    )
    # Check that store_backend_id is created on instantiation, before being accessed
    desired_directory_tree_str = f"""\
{test_dir}/
    dummy_str/
        .ge_store_backend_id
"""
    assert gen_directory_tree_str(project_path) == desired_directory_tree_str
    check_store_backend_store_backend_id_functionality(store_backend=tuple_filesystem_store_backend)
    assert gen_directory_tree_str(project_path) == desired_directory_tree_str

    # Repeat the above with a filepath template
    full_test_dir_with_file_template = tmp_path_factory.mktemp(
        "test_StoreBackend_id_initialization__dir"
    )
    test_dir_with_file_template = full_test_dir_with_file_template.parts[-1]
    project_path_with_filepath_template = str(full_test_dir_with_file_template)

    tuple_filesystem_store_backend_with_filepath_template = TupleFilesystemStoreBackend(
        root_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
        base_directory=project_path_with_filepath_template,
        filepath_template="my_file_{0}",
    )
    check_store_backend_store_backend_id_functionality(
        store_backend=tuple_filesystem_store_backend_with_filepath_template
    )
    assert (
        gen_directory_tree_str(project_path_with_filepath_template)
        == f"""\
{test_dir_with_file_template}/
    .ge_store_backend_id
"""
    )

    # Create a new store with the same config and make sure it reports the same store_backend_id
    tuple_filesystem_store_backend_duplicate = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
        # filepath_template="my_file_{0}",
    )
    check_store_backend_store_backend_id_functionality(
        store_backend=tuple_filesystem_store_backend_duplicate
    )
    assert (
        tuple_filesystem_store_backend.store_backend_id
        == tuple_filesystem_store_backend_duplicate.store_backend_id
    )


@pytest.mark.unit
def test_InMemoryStoreBackend():
    my_store = InMemoryStoreBackend()

    my_key = ("A",)
    with pytest.raises(InvalidKeyError):
        my_store.get(my_key)

    my_store.set(my_key, "aaa")
    assert my_store.get(my_key) == "aaa"

    my_store.set(("B",), {"x": 1})

    assert my_store.has_key(my_key) is True
    assert my_store.has_key(("B",)) is True
    assert my_store.has_key(("A",)) is True
    assert my_store.has_key(("C",)) is False
    assert my_store.list_keys() == [(".ge_store_backend_id",), ("A",), ("B",)]

    with pytest.raises(StoreError):
        my_store.get_url_for_key(my_key)


@pytest.mark.filesystem
def test_tuple_filesystem_store_filepath_prefix_error(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("test_tuple_filesystem_store_filepath_prefix_error"))
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=project_path,
            base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
            filepath_prefix="invalid_prefix_ends_with/",
        )
    assert "filepath_prefix may not end with" in e.value.message

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=project_path,
            base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
            filepath_prefix="invalid_prefix_ends_with\\",
        )
    assert "filepath_prefix may not end with" in e.value.message


@pytest.mark.filesystem
def test_TupleFilesystemStoreBackend_construction_tolerates_read_only_parent_directory(
    tmp_path,
):
    """Constructing a store backend whose parent directory cannot be created because it sits
    under a read-only directory must not raise: the directory is only needed at write time, and
    TupleFilesystemStoreBackend._set/_move re-create their own leaf directory before writing, so
    a genuine write against a read-only filesystem still fails clearly later.
    """  # FIXME CoP
    readonly_parent = tmp_path / "readonly_parent"
    readonly_parent.mkdir()
    base_directory = readonly_parent / "nested" / "leaf"

    readonly_parent.chmod(0o555)
    try:
        # Some environments (root, or filesystems that ignore mode bits) do not enforce
        # directory write permissions; skip cleanly rather than false-failing there.
        probe = readonly_parent / "write_probe"
        try:
            probe.mkdir()
        except OSError:
            pass
        else:
            probe.rmdir()
            pytest.skip("directory permissions are not enforced in this environment")

        # suppress_store_backend_id avoids a second, later write (the store_backend_id file)
        # that would also hit the read-only parent -- this test targets only the eager
        # directory-creation guard exercised directly by __init__.
        store_backend = TupleFilesystemStoreBackend(
            base_directory=str(base_directory),
            suppress_store_backend_id=True,
        )

        assert store_backend.full_base_directory == str(base_directory)
        # The guard defers creation rather than forcing it -- the leaf directory was never made.
        assert not base_directory.parent.exists()
    finally:
        readonly_parent.chmod(0o755)


@pytest.mark.filesystem
def test_TupleFilesystemStoreBackend_construction_reraises_non_read_only_oserror(tmp_path):
    """A genuine non-read-only failure (e.g. out of disk space) during the eager parent-directory
    creation must still raise at construction, exactly as before the read-only tolerance was
    added.
    """  # FIXME CoP
    base_directory = tmp_path / "store"

    def raise_enospc(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    with mock.patch(
        "great_expectations.data_context.store.tuple_store_backend.os.makedirs",
        side_effect=raise_enospc,
    ):
        with pytest.raises(OSError) as exc_info:  # FIXME CoP
            TupleFilesystemStoreBackend(
                base_directory=str(base_directory),
                suppress_store_backend_id=True,
            )
    assert exc_info.value.errno == errno.ENOSPC


@pytest.mark.filesystem
def test_FilesystemStoreBackend_two_way_string_conversion(tmp_path_factory):
    path = str(
        tmp_path_factory.mktemp("test_FilesystemStoreBackend_two_way_string_conversion__dir")
    )
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    my_store = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
        filepath_template="{0}/{1}/{2}/foo-{2}-expectations.txt",
    )

    tuple_ = ("A__a", "B-b", "C")
    converted_string = my_store._convert_key_to_filepath(tuple_)
    assert converted_string == "A__a/B-b/C/foo-C-expectations.txt"

    recovered_key = my_store._convert_filepath_to_key("A__a/B-b/C/foo-C-expectations.txt")
    assert recovered_key == tuple_

    with pytest.raises(ValueError):
        tuple_ = ("A/a", "B-b", "C")
        converted_string = my_store._convert_key_to_filepath(tuple_)


@pytest.mark.filesystem
def test_TupleFilesystemStoreBackend(tmp_path_factory):
    path = "dummy_str"
    full_test_dir = tmp_path_factory.mktemp("test_TupleFilesystemStoreBackend__dir")
    test_dir = full_test_dir.parts[-1]
    project_path = str(full_test_dir)
    base_public_path = "http://www.test.com/"

    my_store = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
        filepath_template="my_file_{0}",
    )

    with pytest.raises(InvalidKeyError):
        my_store.get(("AAA",))

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {(".ge_store_backend_id",), ("AAA",), ("BBB",)}
    assert (
        gen_directory_tree_str(project_path)
        == f"""\
{test_dir}/
    dummy_str/
        .ge_store_backend_id
        my_file_AAA
        my_file_BBB
"""
    )
    my_store.remove_key(("BBB",))
    with pytest.raises(InvalidKeyError):
        assert my_store.get(("BBB",)) == ""

    my_store_with_base_public_path = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
        filepath_template="my_file_{0}",
        base_public_path=base_public_path,
    )
    my_store_with_base_public_path.set(("CCC",), "ccc")
    url = my_store_with_base_public_path.get_public_url_for_key(("CCC",))
    assert url == "http://www.test.com/my_file_CCC"


@pytest.mark.filesystem
def test_TupleFilesystemStoreBackend_get_all(tmp_path_factory):
    path = "dummy_str"
    full_test_dir = tmp_path_factory.mktemp("test_TupleFilesystemStoreBackend__dir")
    project_path = str(full_test_dir)

    my_store = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),  # noqa: PTH118 # FIXME CoP
        filepath_template="my_file_{0}",
    )

    value_a = "aaa"
    value_b = "bbb"

    my_store.set(("AAA",), value_a)
    my_store.set(("BBB",), value_b)

    all_values = my_store.get_all()

    assert sorted(all_values) == [value_a, value_b]


@pytest.mark.filesystem
def test_TupleFilesystemStoreBackend_ignores_jupyter_notebook_checkpoints(
    tmp_path_factory,
):
    full_test_dir = tmp_path_factory.mktemp("things")
    test_dir = full_test_dir.parts[-1]
    project_path = str(full_test_dir)

    checkpoint_dir = os.path.join(project_path, ".ipynb_checkpoints")  # noqa: PTH118 # FIXME CoP
    os.mkdir(checkpoint_dir)  # noqa: PTH102 # FIXME CoP
    assert os.path.isdir(checkpoint_dir)  # noqa: PTH112 # FIXME CoP
    nb_file = os.path.join(checkpoint_dir, "foo.json")  # noqa: PTH118 # FIXME CoP

    with open(nb_file, "w") as f:
        f.write("")
    assert os.path.isfile(nb_file)  # noqa: PTH113 # FIXME CoP
    my_store = TupleFilesystemStoreBackend(
        root_directory=os.path.join(project_path, "dummy_str"),  # noqa: PTH118 # FIXME CoP
        base_directory=project_path,
    )

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    assert (
        gen_directory_tree_str(project_path)
        == f"""\
{test_dir}/
    .ge_store_backend_id
    AAA
    .ipynb_checkpoints/
        foo.json
"""
    )

    assert set(my_store.list_keys()) == {(".ge_store_backend_id",), ("AAA",)}


@pytest.mark.filesystem
def test_InlineStoreBackend(empty_data_context) -> None:
    inline_store_backend: InlineStoreBackend = InlineStoreBackend(
        data_context=empty_data_context,
        resource_type=DataContextVariableSchema.CONFIG_VERSION,
    )
    new_config_version: float = 5.0

    # test invalid .set
    key = DataContextVariableKey()
    tuple_ = key.to_tuple()
    with pytest.raises(StoreBackendError) as e:
        inline_store_backend.set(tuple_, "a_random_string_value")  # Invalid type

    assert "ValueError while calling _set on store backend" in str(e.value)

    # test valid .set
    key = DataContextVariableKey()
    tuple_ = key.to_tuple()
    with mock.patch(
        "great_expectations.data_context.store.InlineStoreBackend._save_changes"
    ) as mock_save:
        inline_store_backend.set(tuple_, new_config_version)

    assert empty_data_context.variables.config.config_version == new_config_version
    assert mock_save.call_count == 1

    # test .get
    key = DataContextVariableKey()
    tuple_ = key.to_tuple()
    ret = inline_store_backend.get(tuple_)
    assert ret == new_config_version

    # test .list_keys
    inline_store_backend = InlineStoreBackend(
        data_context=empty_data_context,
        resource_type=DataContextVariableSchema.ALL_VARIABLES,
    )
    assert sorted(inline_store_backend.list_keys()) == [
        ("analytics_enabled",),
        ("checkpoint_store_name",),
        ("config_variables_file_path",),
        ("config_version",),
        ("data_context_id",),
        ("data_docs_sites",),
        ("expectations_store_name",),
        ("fluent_datasources",),
        ("plugins_directory",),
        ("progress_bars",),
        ("stores",),
        ("validation_results_store_name",),
    ]

    # test .move
    key1 = DataContextVariableKey()
    tuple1 = key1.to_tuple()

    key2 = DataContextVariableKey()
    tuple2 = key2.to_tuple()

    with pytest.raises(StoreBackendError) as e:
        inline_store_backend.move(tuple1, tuple2)

    assert "InlineStoreBackend does not support moving of keys" in str(e.value)

    # test invalid .remove_key
    inline_store_backend = InlineStoreBackend(
        data_context=empty_data_context,
        resource_type=DataContextVariableSchema.PROGRESS_BARS,
    )
    key = DataContextVariableKey(resource_name="profilers")
    tuple_ = key.to_tuple()
    with pytest.raises(StoreBackendError) as e:
        inline_store_backend.remove_key(tuple_)

    assert "Could not find a value associated with key" in str(e.value)

    key = DataContextVariableKey()
    tuple_ = key.to_tuple()
    with pytest.raises(StoreBackendError) as e:
        inline_store_backend.remove_key(tuple_)

    assert "InlineStoreBackend does not support the deletion of top level keys" in str(e.value)

    # test valid .remove_key
    inline_store_backend = InlineStoreBackend(
        data_context=empty_data_context,
        resource_type=DataContextVariableSchema.STORES,
    )
    store_name: str = "my_store"
    store_value: dict = {
        "class_name": "ExpectationsStore",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
        },
    }
    key = DataContextVariableKey(resource_name=store_name)

    tuple_ = key.to_tuple()
    inline_store_backend.set(key=tuple_, value=store_value)
    inline_store_backend.remove_key(tuple_)


@pytest.mark.filesystem
def test_InlineStoreBackend_get_all_success(empty_data_context) -> None:
    inline_store_backend = InlineStoreBackend(
        data_context=empty_data_context,
        resource_type=DataContextVariableSchema.FLUENT_DATASOURCES,
    )

    datasource_config_a = empty_data_context.data_sources.add_pandas(name="a")
    datasource_config_b = empty_data_context.data_sources.add_pandas(name="b")

    inline_store_backend.set(DataContextVariableKey("a").to_tuple(), datasource_config_a)
    inline_store_backend.set(DataContextVariableKey("b").to_tuple(), datasource_config_b)

    all_of_em = inline_store_backend.get_all()

    assert all_of_em == [datasource_config_a, datasource_config_b]


@pytest.mark.filesystem
def test_InlineStoreBackend_get_all_invalid_resource_type(empty_data_context) -> None:
    inline_store_backend = InlineStoreBackend(
        data_context=empty_data_context,
        resource_type=DataContextVariableSchema.ALL_VARIABLES,
    )

    expected_error = "Unsupported resource type: data_context_variables"
    with pytest.raises(StoreBackendError, match=expected_error):
        inline_store_backend.get_all()


@pytest.mark.unit
def test_InMemoryStoreBackend_move_overwrites_key() -> None:
    store_backend = InMemoryStoreBackend()

    key_1 = ("my_key_1",)
    key_2 = ("my_key_2",)

    store_backend.set(key_1, 123)
    assert store_backend.has_key(key_1)
    assert not store_backend.has_key(key_2)

    store_backend.move(key_1, key_2)
    assert not store_backend.has_key(key_1)
    assert store_backend.has_key(key_2)


@pytest.mark.unit
def test_InMemoryStoreBackend_move_nonexistent_key_raises_error() -> None:
    store_backend = InMemoryStoreBackend()

    with pytest.raises(KeyError):
        store_backend.move(("my_fake_key_1",), ("my_fake_key_2",))


@pytest.mark.unit
def test_InMemoryStoreBackend_config_and_defaults() -> None:
    store_backend = InMemoryStoreBackend()
    assert store_backend.config == {
        "class_name": "InMemoryStoreBackend",
        "fixed_length_key": False,
        "module_name": "great_expectations.data_context.store.in_memory_store_backend",
        "suppress_store_backend_id": False,
    }


@pytest.mark.unit
def test_InMemoryStoreBackend_build_Key() -> None:
    store_backend = InMemoryStoreBackend()
    name = "my_backend_key"
    assert store_backend.build_key(name=name) == DataContextVariableKey(resource_name=name)


@pytest.mark.unit
def test_InMemoryStoreBackend_add_success():
    store_backend = InMemoryStoreBackend()
    key = ("foo",)
    value = "bar"

    store_backend.add(key=key, value=value)
    assert key in store_backend.list_keys()


@pytest.mark.unit
def test_InMemoryStoreBackend_add_failure():
    store_backend = InMemoryStoreBackend()
    key = ("foo",)
    value = "bar"

    store_backend.add(key=key, value=value)
    with pytest.raises(StoreBackendError) as e:
        store_backend.add(key=key, value=value)

    assert "Store already has the following key" in str(e.value)


@pytest.mark.unit
def test_InMemoryStoreBackend_update_success():
    store_backend = InMemoryStoreBackend()
    key = ("foo",)
    value = "bar"
    updated_value = "baz"

    store_backend.add(key=key, value=value)
    store_backend.update(key=key, value=updated_value)

    assert store_backend.get(key) == updated_value


@pytest.mark.unit
def test_InMemoryStoreBackend_update_failure():
    store_backend = InMemoryStoreBackend()
    key = ("foo",)
    value = "bar"

    with pytest.raises(StoreBackendError) as e:
        store_backend.update(key=key, value=value)

    assert "Store does not have a value associated the following key" in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize("previous_key_exists", [True, False])
def test_InMemoryStoreBackend_add_or_update(previous_key_exists: bool):
    store_backend = InMemoryStoreBackend()
    key = ("foo",)
    value = "bar"

    if previous_key_exists:
        store_backend.add(key=key, value=None)

    store_backend.add_or_update(key=key, value=value)
    assert store_backend.get(key) == value


@pytest.mark.unit
def test_store_backend_path_special_character_escape():
    path = "/validations/default/pandas_data_asset/20230315T205136.109084Z/default_pandas_datasource-#ephemeral_pandas_asset.html"  # noqa: E501 # FIXME CoP
    escaped_path = StoreBackend._url_path_escape_special_characters(path=path)
    assert (
        escaped_path
        == "/validations/default/pandas_data_asset/20230315T205136.109084Z/default_pandas_datasource-%23ephemeral_pandas_asset.html"  # noqa: E501 # FIXME CoP
    )


@pytest.mark.filesystem
def test_file_backed_store_backends_use_json(empty_data_context):
    context = empty_data_context
    for store in context.stores.values():
        backend = store.store_backend
        assert isinstance(backend, TupleFilesystemStoreBackend)
        assert backend.filepath_suffix == ".json"
