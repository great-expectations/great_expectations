import pathlib

import pytest

from docs.prepare_prior_versions import (
    _prepend_version_info_for_md_absolute_links,
    _prepend_version_info_to_name_for_md_relative_links,
    _update_tag_references_for_correct_version_substitution,
    _use_relative_path_for_imports_substitution,
    _use_relative_path_for_imports_substitution_path_starting_with_forwardslash,
)


@pytest.mark.unit
def test__update_tag_references_for_correct_version_substitution():
    contents = """import data from '../term_tags/terms.json'

<span class="tooltip">
    <a href={'/docs/' + data[props.tag].url}>{props.text}</a>
    <span class="tooltiptext">{data[props.tag].definition}</span>
</span>"""

    version = "0.15.50"
    updated_contents = _update_tag_references_for_correct_version_substitution(
        contents=contents, version=version
    )
    expected_contents = """import data from '../term_tags/terms.json'

<span class="tooltip">
    <a href={'/docs/0.15.50/' + data[props.tag].url}>{props.text}</a>
    <span class="tooltiptext">{data[props.tag].definition}</span>
</span>"""
    assert updated_contents == expected_contents


@pytest.mark.unit
def test__use_relative_path_for_imports_substitution():
    contents = """import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on GCS using Pandas.
"""

    path_to_versioned_docs = pathlib.Path(
        "docs/docusaurus/versioned_docs/version-0.14.13/"
    )
    file_path = pathlib.Path(
        "docs/docusaurus/versioned_docs/version-0.14.13/guides/connecting_to_your_data/cloud/gcs/pandas.md"
    )

    updated_contents = _use_relative_path_for_imports_substitution(
        contents, path_to_versioned_docs, file_path
    )

    expected_contents = """import TabItem from '@theme/TabItem';
import TechnicalTag from '../../../../term_tags/_tag.mdx';

This guide will help you connect to your data stored on GCS using Pandas.
"""

    assert updated_contents == expected_contents


@pytest.mark.unit
def test__use_relative_path_for_imports_substitution_path_starting_with_forwardslash():
    contents = """import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '/docs/term_tags/_tag.mdx';

<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/>
"""

    path_to_versioned_docs = pathlib.Path(
        "docs/docusaurus/versioned_docs/version-0.14.13/"
    )
    file_path = pathlib.Path(
        "docs/docusaurus/versioned_docs/version-0.14.13/tutorials/getting_started/tutorial_connect_to_data.md"
    )

    updated_contents = (
        _use_relative_path_for_imports_substitution_path_starting_with_forwardslash(
            contents, path_to_versioned_docs, file_path
        )
    )

    expected_contents = """import UniversalMap from '../../images/universal_map/_universal_map.mdx';
import TechnicalTag from '../../term_tags/_tag.mdx';

<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/>
"""

    assert updated_contents == expected_contents


@pytest.mark.unit
def test__use_relative_path_for_imports_substitution_path_starting_with_forwardslash_same_directory():
    """Tests for relative path where the imported file is in the same directory as the file being updated."""

    contents = """---
id: glossary
title: "Glossary of Terms"
---

import CLIRemoval from '/components/warnings/_cli_removal.md'

<CLIRemoval />
"""

    path_to_versioned_docs = pathlib.Path(
        "docs/docusaurus/versioned_docs/version-0.16.16/"
    )
    file_path = pathlib.Path(
        "docs/docusaurus/versioned_docs/version-0.16.16/glossary.md"
    )

    updated_contents = (
        _use_relative_path_for_imports_substitution_path_starting_with_forwardslash(
            contents, path_to_versioned_docs, file_path
        )
    )

    expected_contents = """---
id: glossary
title: "Glossary of Terms"
---

import CLIRemoval from './components/warnings/_cli_removal.md'

<CLIRemoval />
"""

    assert updated_contents == expected_contents


@pytest.mark.unit
def test__prepend_version_info_to_name_for_md_relative_links():
    contents = """For more information on pre-configuring a Checkpoint with a Batch Request and Expectation Suite, see [Manage Checkpoints](../../../../docs/guides/validation/checkpoints/checkpoint_lp.md)."""

    version = "0.16.16"
    updated_contents = _prepend_version_info_to_name_for_md_relative_links(
        contents, version
    )
    expected_contents = """For more information on pre-configuring a Checkpoint with a Batch Request and Expectation Suite, see [Manage Checkpoints](../../../../docs/guides/validation/checkpoints/checkpoint_lp.md)."""
    assert updated_contents == expected_contents


class TestPrependVersionInfoForMdAbsoluteLinks:
    @pytest.mark.unit
    def test__prepend_version_info_for_md_absolute_links(self):
        contents = """- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
    - [How to use Great Expectations in Databricks](/docs/tutorials/getting_started/how_to_use_great_expectations_in_databricks)
    """
        version = "0.16.16"
        expected_contents = """- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/0.16.16/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
    - [How to use Great Expectations in Databricks](/docs/0.16.16/tutorials/getting_started/how_to_use_great_expectations_in_databricks)
    """

        updated_contents = _prepend_version_info_for_md_absolute_links(
            contents, version
        )
        assert updated_contents == expected_contents

    @pytest.mark.unit
    def test__prepend_version_info_for_md_absolute_links_doesnt_update_if_version_already_exists(
        self,
    ):
        contents = """- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/0.15.50/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
    - [How to use Great Expectations in Databricks](/docs/0.15.50/tutorials/getting_started/how_to_use_great_expectations_in_databricks)
    """
        version = "0.16.16"
        expected_contents = """- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/0.15.50/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
    - [How to use Great Expectations in Databricks](/docs/0.15.50/tutorials/getting_started/how_to_use_great_expectations_in_databricks)
    """

        updated_contents = _prepend_version_info_for_md_absolute_links(
            contents, version
        )
        assert updated_contents == expected_contents

    @pytest.mark.unit
    def test__prepend_version_info_for_md_absolute_links_doesnt_update_if_version_already_exists_mixed_versions(
        self,
    ):
        contents = """- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
    - [How to use Great Expectations in Databricks](/docs/0.15.50/deployment_patterns/how_to_use_great_expectations_in_databricks)
    """
        version = "0.16.16"
        expected_contents = """- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/0.16.16/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
    - [How to use Great Expectations in Databricks](/docs/0.15.50/deployment_patterns/how_to_use_great_expectations_in_databricks)
    """

        updated_contents = _prepend_version_info_for_md_absolute_links(
            contents, version
        )
        assert updated_contents == expected_contents

    @pytest.mark.unit
    def test__prepend_version_info_for_md_absoulte_links_updates_two_links_on_the_same_line(
        self,
    ):
        contents = """[Run a Checkpoint](/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) to store results in the new Validation Results Store on S3 then visualize the results by [re-building Data Docs](/docs/terms/data_docs)."""
        version = "0.16.16"
        expected_contents = """[Run a Checkpoint](/docs/0.16.16/guides/validation/how_to_validate_data_by_running_a_checkpoint) to store results in the new Validation Results Store on S3 then visualize the results by [re-building Data Docs](/docs/0.16.16/terms/data_docs)."""
        updated_contents = _prepend_version_info_for_md_absolute_links(
            contents, version
        )
        assert updated_contents == expected_contents

    @pytest.mark.unit
    def test__prepend_version_info_for_md_absoulte_links_updates_multiple_mixed_links(
        self,
    ):
        """Links that are already versioned should not be updated, even when there are multiple links on the same line that should be updated."""
        contents = """[Link Text 1](/docs/guides/link_1) text [Link Text 2](/docs/guides/link_2) text [Link Text 3](/docs/0.1.2/guides/link_3) text [Link Text 4](/docs/guides/link_4) text."""
        version = "0.16.16"
        expected_contents = """[Link Text 1](/docs/0.16.16/guides/link_1) text [Link Text 2](/docs/0.16.16/guides/link_2) text [Link Text 3](/docs/0.1.2/guides/link_3) text [Link Text 4](/docs/0.16.16/guides/link_4) text."""
        updated_contents = _prepend_version_info_for_md_absolute_links(
            contents, version
        )
        assert updated_contents == expected_contents
