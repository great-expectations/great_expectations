import os
from unittest import mock

import pytest

from great_expectations import DataContext
from great_expectations.data_context import BaseDataContext
from great_expectations.exceptions import DataContextError


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_open_docs_with_no_site(mock_webbrowser, context_with_no_sites):
    context = context_with_no_sites

    context.open_data_docs()
    assert mock_webbrowser.call_count == 0


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_open_docs_with_non_existent_site_raises_error(
    mock_webbrowser, empty_data_context
):
    context = empty_data_context
    with pytest.raises(DataContextError):
        context.open_data_docs(site_name="foo")
    assert mock_webbrowser.call_count == 0


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
    context._project_config = config
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
        assert os.path.isfile(
            os.path.join(
                context.root_directory,
                context.GE_UNCOMMITTED_DIR,
                "data_docs",
                site,
                "index.html",
            )
        )

    return context


@pytest.fixture
def context_with_multiple_local_sites_and_s3_site(empty_data_context):
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
        "s3_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "foo",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
    }
    config.data_docs_sites = multi_sites
    context._project_config = config
    obs = context.get_docs_sites_urls()
    assert len(obs) == 2
    assert obs[0]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )
    assert obs[0]["site_name"] == "local_site"

    assert obs[1]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/another_local_site/index.html"
    )
    assert obs[1]["site_name"] == "another_local_site"

    return context


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


def test_get_docs_sites_urls_with_no_sites(context_with_no_sites):
    assert context_with_no_sites.get_docs_sites_urls() == []


def test_get_docs_sites_urls_with_no_sites_specify_one(context_with_no_sites):
    assert context_with_no_sites.get_docs_sites_urls(site_name="foo") == []


def test_get_docs_sites_urls_with_non_existent_site_raises_error(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    with pytest.raises(DataContextError):
        context.get_docs_sites_urls(site_name="not_a_real_site")


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


def test_clean_data_docs_on_context_with_no_sites_raises_error(
    context_with_no_sites,
):
    context = context_with_no_sites
    with pytest.raises(DataContextError):
        context.clean_data_docs()


def test_clean_data_docs_on_context_with_multiple_sites_with_no_site_name_cleans_all_sites_and_returns_true(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    assert context.clean_data_docs() is True
    for site in ["local_site", "another_local_site"]:
        assert not os.path.isfile(
            os.path.join(
                context.root_directory,
                context.GE_UNCOMMITTED_DIR,
                "data_docs",
                site,
                "index.html",
            )
        )


def test_clean_data_docs_on_context_with_multiple_sites_with_existing_site_name_cleans_selected_site_and_returns_true(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    assert context.clean_data_docs(site_name="another_local_site") is True
    data_docs_dir = os.path.join(
        context.root_directory, context.GE_UNCOMMITTED_DIR, "data_docs"
    )
    assert not os.path.isfile(
        os.path.join(data_docs_dir, "another_local_site", "index.html")
    )
    assert os.path.isfile(os.path.join(data_docs_dir, "local_site", "index.html"))


def test_clean_data_docs_on_context_with_multiple_sites_with_non_existent_site_name_raises_error(
    context_with_multiple_built_sites,
):
    context = context_with_multiple_built_sites
    with pytest.raises(DataContextError):
        assert context.clean_data_docs(site_name="not_a_real_site")


def test_existing_local_data_docs_urls_returns_url_on_project_with_no_datasources_and_a_site_configured(
    tmp_path_factory,
):
    """
    This test ensures that a url will be returned for a default site even if a
    datasource is not configured, and docs are not built.
    """
    empty_directory = str(tmp_path_factory.mktemp("another_empty_project"))
    DataContext.create(empty_directory)
    context = DataContext(os.path.join(empty_directory, DataContext.GE_DIR))

    obs = context.get_docs_sites_urls(only_if_exists=False)
    assert len(obs) == 1
    assert obs[0]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )


def test_existing_local_data_docs_urls_returns_single_url_from_customized_local_site(
    tmp_path_factory,
):
    empty_directory = str(tmp_path_factory.mktemp("yo_yo"))
    DataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, DataContext.GE_DIR)
    context = DataContext(ge_dir)

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
    context = DataContext(ge_dir)
    context.build_data_docs()

    expected_path = os.path.join(
        ge_dir, "uncommitted/data_docs/some/local/path/index.html"
    )
    assert os.path.isfile(expected_path)

    obs = context.get_docs_sites_urls()
    assert obs == [{"site_name": "my_rad_site", "site_url": f"file://{expected_path}"}]


def test_existing_local_data_docs_urls_returns_multiple_urls_from_customized_local_site(
    tmp_path_factory,
):
    empty_directory = str(tmp_path_factory.mktemp("yo_yo_ma"))
    DataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, DataContext.GE_DIR)
    context = DataContext(ge_dir)

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
    context = DataContext(ge_dir)
    context.build_data_docs()
    data_docs_dir = os.path.join(ge_dir, "uncommitted/data_docs/")

    path_1 = os.path.join(data_docs_dir, "some/path/index.html")
    path_2 = os.path.join(data_docs_dir, "another/path/index.html")
    for expected_path in [path_1, path_2]:
        assert os.path.isfile(expected_path)

    obs = context.get_docs_sites_urls()

    assert obs == [
        {"site_name": "my_rad_site", "site_url": f"file://{path_1}"},
        {
            "site_name": "another_just_amazing_site",
            "site_url": f"file://{path_2}",
        },
    ]


def test_build_data_docs_skipping_index_does_not_build_index(
    tmp_path_factory,
):
    # TODO What's the latest and greatest way to use configs rather than my hackery?
    empty_directory = str(tmp_path_factory.mktemp("empty"))
    DataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, DataContext.GE_DIR)
    context = DataContext(ge_dir)
    config = context.get_config()
    config.data_docs_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": os.path.join("uncommitted", "data_docs"),
            },
        },
    }
    context._project_config = config

    # TODO Workaround project config programmatic config manipulation
    #  statefulness issues by writing to disk and re-upping a new context
    context._save_project_config()
    del context
    context = DataContext(ge_dir)
    data_docs_dir = os.path.join(ge_dir, "uncommitted", "data_docs")
    index_path = os.path.join(data_docs_dir, "index.html")
    assert not os.path.isfile(index_path)

    context.build_data_docs(build_index=False)
    assert os.path.isdir(os.path.join(data_docs_dir, "static"))
    assert not os.path.isfile(index_path)


def test_get_site_names_with_no_sites(tmpdir, basic_data_context_config):
    context = BaseDataContext(basic_data_context_config, context_root_dir=tmpdir)
    assert context.get_site_names() == []


def test_get_site_names_with_site(titanic_data_context_stats_enabled_config_version_3):
    context = titanic_data_context_stats_enabled_config_version_3
    assert context.get_site_names() == ["local_site"]


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
    context = BaseDataContext(basic_data_context_config, context_root_dir=tmpdir)
    assert context.get_site_names() == ["site-0", "site-1", "site-2"]
