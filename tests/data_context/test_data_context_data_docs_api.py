from unittest import mock

import pytest

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
def context_with_multiple_sites(empty_data_context):
    context = empty_data_context
    config = context._project_config_with_variables_substituted
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

    return context


@pytest.fixture
def context_with_multiple_local_sites_and_s3_site(empty_data_context):
    context = empty_data_context
    config = context._project_config_with_variables_substituted
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
def test_open_docs_with_two_local_sites(mock_webbrowser, context_with_multiple_sites):
    context = context_with_multiple_sites
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
    mock_webbrowser, context_with_multiple_sites
):
    context = context_with_multiple_sites
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
    context_with_multiple_sites,
):
    context = context_with_multiple_sites
    with pytest.raises(DataContextError):
        context.get_docs_sites_urls(site_name="not_a_real_site")


def test_get_docs_sites_urls_with_two_local_sites_specify_one(
    context_with_multiple_sites,
):
    context = context_with_multiple_sites
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
