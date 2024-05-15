import copy
from unittest import mock

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context import EphemeralDataContext


@pytest.fixture
def new_site_config() -> dict:
    return {
        "class_name": "SiteBuilder",
        "module_name": "great_expectations.render.renderer.site_builder",
        "store_backend": {
            "module_name": "great_expectations.data_context.store.tuple_store_backend",
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": "/my_new_site/",
        },
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    }


class TestAddDataDocsSite:
    @pytest.mark.unit
    def test_add_data_docs_site(
        self,
        ephemeral_context_with_defaults: EphemeralDataContext,
        new_site_config: dict,
    ):
        # Add a new site
        new_site_name = "my_new_site"
        ephemeral_context_with_defaults.add_data_docs_site(
            site_name=new_site_name, site_config=new_site_config
        )

        # Check that the new site is present
        assert new_site_name in ephemeral_context_with_defaults.get_site_names()

    @pytest.mark.unit
    def test_add_data_docs_site_persists(
        self,
        ephemeral_context_with_defaults: EphemeralDataContext,
        new_site_config: dict,
    ):
        new_site_name = "my_new_site"
        with mock.patch(
            "great_expectations.data_context.EphemeralDataContext._save_project_config"
        ) as mock_save_project_config:
            ephemeral_context_with_defaults.add_data_docs_site(
                site_name=new_site_name, site_config=new_site_config
            )

        mock_save_project_config.assert_called_once()

    @pytest.mark.unit
    def test_add_data_docs_site_already_existing_site_raises_exception(
        self,
        ephemeral_context_with_defaults: EphemeralDataContext,
        new_site_config: dict,
    ):
        # Check fixture configuration
        existing_site_name = "local_site"
        assert existing_site_name in ephemeral_context_with_defaults.get_site_names()

        with pytest.raises(gx_exceptions.InvalidKeyError) as e:
            new_site_name = existing_site_name
            ephemeral_context_with_defaults.add_data_docs_site(
                site_name=new_site_name, site_config=new_site_config
            )

        assert "Data Docs Site `local_site` already exists in the Data Context." in str(e.value)


class TestListDataDocsSites:
    @pytest.mark.unit
    def test_list_data_docs_sites(self, ephemeral_context_with_defaults: EphemeralDataContext):
        site_names = [d for d in ephemeral_context_with_defaults.list_data_docs_sites()]
        assert site_names == ["local_site"]


class TestUpdateDataDocsSite:
    @pytest.mark.unit
    def test_update_data_docs_site(
        self,
        ephemeral_context_with_defaults: EphemeralDataContext,
        new_site_config: dict,
    ):
        # Add a new site
        new_site_name = "my_new_site"
        ephemeral_context_with_defaults.add_data_docs_site(
            site_name=new_site_name, site_config=new_site_config
        )

        # Update the new site
        updated_site_config = copy.deepcopy(new_site_config)
        updated_site_config["store_backend"]["base_directory"] = "/my_updated_site/"

        ephemeral_context_with_defaults.update_data_docs_site(new_site_name, updated_site_config)

        # Check the updated site config
        sites = ephemeral_context_with_defaults.variables.data_docs_sites
        assert sites[new_site_name]["store_backend"]["base_directory"] == "/my_updated_site/"

    @pytest.mark.unit
    def test_update_data_docs_site_persists(
        self,
        ephemeral_context_with_defaults: EphemeralDataContext,
        new_site_config: dict,
    ):
        # Add a new site
        new_site_name = "my_new_site"
        ephemeral_context_with_defaults.add_data_docs_site(
            site_name=new_site_name, site_config=new_site_config
        )

        # Update the new site
        updated_site_config = copy.deepcopy(new_site_config)
        updated_site_config["store_backend"]["base_directory"] = "/my_updated_site/"

        with mock.patch(
            "great_expectations.data_context.EphemeralDataContext._save_project_config"
        ) as mock_save_project_config:
            ephemeral_context_with_defaults.update_data_docs_site(
                new_site_name, updated_site_config
            )

        mock_save_project_config.assert_called_once()

    @pytest.mark.unit
    def test_update_data_docs_site_missing_site_raises_exception(
        self,
        ephemeral_context_with_defaults: EphemeralDataContext,
        new_site_config: dict,
    ):
        # Check fixture configuration
        assert "missing" not in ephemeral_context_with_defaults.get_site_names()

        with pytest.raises(gx_exceptions.InvalidKeyError) as e:
            ephemeral_context_with_defaults.update_data_docs_site(
                site_name="missing", site_config=new_site_config
            )

        assert "Data Docs Site `missing` does not already exist in the Data Context." in str(
            e.value
        )


class TestDeleteDataDocsSite:
    @pytest.mark.unit
    def test_delete_data_docs_site(self, ephemeral_context_with_defaults: EphemeralDataContext):
        # Check fixture configuration
        existing_site_name = "local_site"
        assert existing_site_name in ephemeral_context_with_defaults.get_site_names()

        ephemeral_context_with_defaults.delete_data_docs_site(existing_site_name)

        # Check that the site is no longer present
        assert existing_site_name not in ephemeral_context_with_defaults.get_site_names()

    @pytest.mark.unit
    def test_delete_data_docs_site_persists(
        self, ephemeral_context_with_defaults: EphemeralDataContext
    ):
        # Check fixture configuration
        existing_site_name = "local_site"
        assert existing_site_name in ephemeral_context_with_defaults.get_site_names()

        with mock.patch(
            "great_expectations.data_context.EphemeralDataContext._save_project_config"
        ) as mock_save_project_config:
            ephemeral_context_with_defaults.delete_data_docs_site(existing_site_name)

        mock_save_project_config.assert_called_once()

    @pytest.mark.unit
    def test_delete_data_docs_site_missing_site_raises_exception(
        self,
        ephemeral_context_with_defaults: EphemeralDataContext,
    ):
        # Check fixture configuration
        assert "missing" not in ephemeral_context_with_defaults.get_site_names()

        with pytest.raises(gx_exceptions.InvalidKeyError) as e:
            ephemeral_context_with_defaults.delete_data_docs_site("missing")

        assert "Data Docs Site `missing` does not already exist in the Data Context." in str(
            e.value
        )
