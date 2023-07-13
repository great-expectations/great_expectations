import os
from unittest import mock

import pytest

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.exceptions import DataContextError
from great_expectations.util import get_context


@pytest.mark.unit
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_open_docs_with_no_site(mock_webbrowser, context_with_no_sites):
    context = context_with_no_sites

    context.open_data_docs()
    assert mock_webbrowser.call_count == 0


@pytest.mark.unit
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_open_docs_with_non_existent_site_raises_error(
    mock_webbrowser, empty_data_context
):
    context = empty_data_context
    with pytest.raises(DataContextError):
        context.open_data_docs(site_name="foo")
    assert mock_webbrowser.call_count == 0


@pytest.mark.filesystem
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_open_docs_with_single_local_site(mock_webbrowser, empty_data_context):
    context = empty_data_context
    obs = context.get_docs_sites_urls(only_if_exists=False)
    assert len(obs) == 1
    assert obs[0]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )
    assert obs[0]["site_name"] == "local_site"

    context.open_data_docs(only_if_exists=False)
    assert mock_webbrowser.call_count == 1
    call = mock_webbrowser.call_args_list[0][0][0]
    assert call.startswith("file:///")
    assert call.endswith(
        "/great_expectations/uncommitted/data_docs/local_site/index.html"
    )


@pytest.mark.unit
def test_get_context_no_args_successfully_builds_and_opens_docs():
    context = get_context()

    sites = context.build_data_docs()
    assert len(sites) == 1

    with mock.patch("webbrowser.open") as mock_open:
        context.open_data_docs()
    mock_open.assert_called_once()


@pytest.fixture
def context_with_multiple_built_sites(empty_data_context):
    context = empty_data_context
    config = context.project_config_with_variables_substituted
    multi_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/local_site/",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
        "another_local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/another_local_site/",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
    }
    config.data_docs_sites = multi_sites
    context.variables.config = config
    context.build_data_docs()
    obs = context.get_docs_sites_urls(only_if_exists=False)
    assert len(obs) == 2
    assert obs[0]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )
    assert obs[0]["site_name"] == "local_site"

    assert obs[1]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/another_local_site/index.html"
    )
    assert obs[1]["site_name"] == "another_local_site"
    for site in ["local_site", "another_local_site"]:
        assert os.path.isfile(  # noqa: PTH113
            os.path.join(  # noqa: PTH118
                context.root_directory,
                context.GX_UNCOMMITTED_DIR,
                "data_docs",
                site,
                "index.html",
            )
        )

    return context


@pytest.mark.unit
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_open_docs_with_two_local_sites(
    mock_webbrowser, context_with_multiple_built_sites
):
    context = context_with_multiple_built_sites
    context.open_data_docs(only_if_exists=False)
    assert mock_webbrowser.call_count == 2
    first_call = mock_webbrowser.call_args_list[0][0][0]
    assert first_call.startswith("file:///")
    assert first_call.endswith(
        "/great_expectations/uncommitted/data_docs/local_site/index.html"
    )
    second_call = mock_webbrowser.call_args_list[1][0][0]
    assert second_call.startswith("file:///")
    assert second_call.endswith(
        "/great_expectations/uncommitted/data_docs/another_local_site/index.html"
    )


@pytest.mark.unit
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_open_docs_with_two_local_sites_specify_open_one(
    mock_webbrowser, context_with_multiple_built_sites
):
    context = context_with_multiple_built_sites
    context.open_data_docs(site_name="another_local_site", only_if_exists=False)

    assert mock_webbrowser.call_count == 1
    call = mock_webbrowser.call_args_list[0][0][0]
    assert call.startswith("file:///")
    assert call.endswith(
        "/great_expectations/uncommitted/data_docs/another_local_site/index.html"
    )


@pytest.fixture
def context_with_no_sites(empty_data_context):
    context = empty_data_context
    context._project_config["data_docs_sites"] = None
    return context


@pytest.mark.unit
def test_get_docs_sites_urls_with_no_sites(context_with_no_sites):
    assert context_with_no_sites.get_docs_sites_urls() == []


@pytest.mark.unit
def test_get_docs_sites_urls_with_no_sites_specify_one(context_with_no_sites):
    assert context_with_no_sites.get_docs_sites_urls(site_name="foo") == []


@pytest.mark.unit
def test_get_docs_sites_urls_with_non_existent_site_raises_error(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    with pytest.raises(DataContextError):
        context.get_docs_sites_urls(site_name="not_a_real_site")


@pytest.mark.filesystem
def test_get_docs_sites_urls_with_two_local_sites_specify_one(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    obs = context.get_docs_sites_urls(
        site_name="another_local_site", only_if_exists=False
    )
    assert len(obs) == 1
    assert obs[0]["site_name"] == "another_local_site"

    url = obs[0]["site_url"]
    assert url.startswith("file:///")
    assert url.endswith(
        "/great_expectations/uncommitted/data_docs/another_local_site/index.html"
    )


@pytest.mark.unit
def test_clean_data_docs_on_context_with_no_sites_raises_error(
    context_with_no_sites,
):
    context = context_with_no_sites
    with pytest.raises(DataContextError):
        context.clean_data_docs()


@pytest.mark.filesystem
def test_clean_data_docs_on_context_with_multiple_sites_with_no_site_name_cleans_all_sites_and_returns_true(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    assert context.clean_data_docs() is True
    for site in ["local_site", "another_local_site"]:
        assert not os.path.isfile(  # noqa: PTH113
            os.path.join(  # noqa: PTH118
                context.root_directory,
                context.GX_UNCOMMITTED_DIR,
                "data_docs",
                site,
                "index.html",
            )
        )


@pytest.mark.filesystem
def test_clean_data_docs_on_context_with_multiple_sites_with_existing_site_name_cleans_selected_site_and_returns_true(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    assert context.clean_data_docs(site_name="another_local_site") is True
    data_docs_dir = os.path.join(  # noqa: PTH118
        context.root_directory, context.GX_UNCOMMITTED_DIR, "data_docs"
    )
    assert not os.path.isfile(  # noqa: PTH113
        os.path.join(data_docs_dir, "another_local_site", "index.html")  # noqa: PTH118
    )
    assert os.path.isfile(  # noqa: PTH113
        os.path.join(data_docs_dir, "local_site", "index.html")  # noqa: PTH118
    )


@pytest.mark.filesystem
def test_clean_data_docs_on_context_with_multiple_sites_with_non_existent_site_name_raises_error(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    with pytest.raises(DataContextError):
        assert context.clean_data_docs(site_name="not_a_real_site")


@pytest.mark.filesystem
def test_existing_local_data_docs_urls_returns_url_on_project_with_no_datasources_and_a_site_configured(
    tmp_path_factory,
):
    """
    This test ensures that a url will be returned for a default site even if a
    datasource is not configured, and docs are not built.
    """
    empty_directory = str(tmp_path_factory.mktemp("another_empty_project"))
    FileDataContext.create(empty_directory)
    context = get_context(
        context_root_dir=os.path.join(  # noqa: PTH118
            empty_directory, FileDataContext.GX_DIR
        )
    )

    obs = context.get_docs_sites_urls(only_if_exists=False)
    assert len(obs) == 1
    assert obs[0]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )


@pytest.mark.filesystem
def test_existing_local_data_docs_urls_returns_single_url_from_customized_local_site(
    tmp_path_factory,
):
    empty_directory = str(tmp_path_factory.mktemp("yo_yo"))
    FileDataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, FileDataContext.GX_DIR)  # noqa: PTH118
    context = get_context(context_root_dir=ge_dir)

    context._project_config["data_docs_sites"] = {
        "my_rad_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/some/local/path/",
            },
        }
    }

    # TODO Workaround project config programmatic config manipulation
    #  statefulness issues by writing to disk and re-upping a new context
    context._save_project_config()
    context = get_context(context_root_dir=ge_dir)
    context.build_data_docs()

    expected_path = os.path.join(  # noqa: PTH118
        ge_dir, "uncommitted/data_docs/some/local/path/index.html"
    )
    assert os.path.isfile(expected_path)  # noqa: PTH113

    obs = context.get_docs_sites_urls()
    assert obs == [{"site_name": "my_rad_site", "site_url": f"file://{expected_path}"}]


@pytest.mark.filesystem
def test_existing_local_data_docs_urls_returns_multiple_urls_from_customized_local_site(
    tmp_path_factory,
):
    empty_directory = str(tmp_path_factory.mktemp("yo_yo_ma"))
    FileDataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, FileDataContext.GX_DIR)  # noqa: PTH118
    context = get_context(context_root_dir=ge_dir)

    context._project_config["data_docs_sites"] = {
        "my_rad_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/some/path/",
            },
        },
        "another_just_amazing_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/another/path/",
            },
        },
    }

    # TODO Workaround project config programmatic config manipulation
    #  statefulness issues by writing to disk and re-upping a new context
    context._save_project_config()
    context = get_context(context_root_dir=ge_dir)
    context.build_data_docs()
    data_docs_dir = os.path.join(ge_dir, "uncommitted/data_docs/")  # noqa: PTH118

    path_1 = os.path.join(data_docs_dir, "some/path/index.html")  # noqa: PTH118
    path_2 = os.path.join(data_docs_dir, "another/path/index.html")  # noqa: PTH118
    for expected_path in [path_1, path_2]:
        assert os.path.isfile(expected_path)  # noqa: PTH113

    obs = context.get_docs_sites_urls()

    assert obs == [
        {"site_name": "my_rad_site", "site_url": f"file://{path_1}"},
        {
            "site_name": "another_just_amazing_site",
            "site_url": f"file://{path_2}",
        },
    ]


@pytest.mark.filesystem
def test_build_data_docs_skipping_index_does_not_build_index(
    tmp_path_factory,
):
    # TODO What's the latest and greatest way to use configs rather than my hackery?
    empty_directory = str(tmp_path_factory.mktemp("empty"))
    FileDataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, FileDataContext.GX_DIR)  # noqa: PTH118
    context = get_context(context_root_dir=ge_dir)
    config = context.get_config()
    config.data_docs_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": os.path.join(  # noqa: PTH118
                    "uncommitted", "data_docs"
                ),
            },
        },
    }
    context._project_config = config
    # TODO Workaround project config programmatic config manipulation
    #  statefulness issues by writing to disk and re-upping a new context
    context._save_project_config()
    del context
    context = get_context(context_root_dir=ge_dir)
    data_docs_dir = os.path.join(ge_dir, "uncommitted", "data_docs")  # noqa: PTH118
    index_path = os.path.join(data_docs_dir, "index.html")  # noqa: PTH118
    assert not os.path.isfile(index_path)  # noqa: PTH113

    context.build_data_docs(build_index=False)
    assert os.path.isdir(os.path.join(data_docs_dir, "static"))  # noqa: PTH112, PTH118
    assert not os.path.isfile(index_path)  # noqa: PTH113


@pytest.mark.unit
def test_get_site_names_with_no_sites(tmpdir, basic_data_context_config):
    context = get_context(basic_data_context_config, context_root_dir=tmpdir)
    assert context.get_site_names() == []


@pytest.mark.unit
def test_get_site_names_with_site(titanic_data_context_stats_enabled_config_version_3):
    context = titanic_data_context_stats_enabled_config_version_3
    assert context.get_site_names() == ["local_site"]


@pytest.mark.filesystem
def test_get_site_names_with_three_sites(tmpdir, basic_data_context_config):
    basic_data_context_config.data_docs_sites = {}
    for i in range(3):
        basic_data_context_config.data_docs_sites[f"site-{i}"] = {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": f"uncommitted/data_docs/site-{i}/",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        }
    context = get_context(basic_data_context_config, context_root_dir=tmpdir)
    assert context.get_site_names() == ["site-0", "site-1", "site-2"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_view_validation_result(
    checkpoint_result: CheckpointResult,
):
    context = get_context()

    with mock.patch("webbrowser.open") as mock_open, mock.patch(
        "great_expectations.data_context.store.StoreBackend.has_key", return_value=True
    ):
        context.view_validation_result(checkpoint_result)

    mock_open.assert_called_once()

    url_used = mock_open.call_args[0][0]
    assert url_used.startswith("file:///")
    assert url_used.endswith("default_pandas_datasource-%23ephemeral_pandas_asset.html")
