import copy
import json
from pprint import pprint

from great_expectations._docs_decorators import _public_api_registry


def find_classes_that_need_public_api_decorator() -> list[str]:
    classes = []
    for class_, methods in _public_api_registry.items():
        if "__init__" not in methods:
            classes.append(class_)

    return sorted(classes)


def write_public_api_registry_file() -> None:
    registry = copy.deepcopy(_public_api_registry)
    for k, v in registry.items():
        registry[k] = sorted(v)

    with open("public_api_registry.json", "w") as f:
        json.dump(registry, f, indent=2)


if __name__ == "__main__":
    classes = find_classes_that_need_public_api_decorator()
    pprint(classes)
    """
    great_expectations.checkpoint.checkpoint.CheckpointResult
    great_expectations.data_context.data_context.abstract_data_context.AbstractDataContext,
    great_expectations.data_context.types.base.AssetConfig,
    great_expectations.data_context.types.base.BaseYamlConfig,
    great_expectations.data_context.types.base.DataConnectorConfig,
    great_expectations.data_context.types.base.GXCloudConfig,
    great_expectations.datasource.fluent.data_asset.path.directory_asset.DirectoryDataAsset,
    great_expectations.datasource.fluent.data_asset.path.file_asset.FileDataAsset,
    great_expectations.datasource.fluent.interfaces.DataAsset,
    great_expectations.datasource.fluent.pandas_datasource._PandasDataAsset,
    great_expectations.datasource.fluent.spark_datasource.DataFrameAsset,
    great_expectations.datasource.fluent.sql_datasource._SQLAsset,
    great_expectations.render.components.RenderedAtomicContent,
    great_expectations.render.components.RenderedAtomicValue,
    great_expectations.render.components.RenderedAtomicValueGraph,
    great_expectations.render.components.RenderedBootstrapTableContent,
    great_expectations.render.components.RenderedBulletListContent,
    great_expectations.render.components.RenderedComponentContent,
    great_expectations.render.components.RenderedContent,
    great_expectations.render.components.RenderedContentBlockContainer,
    great_expectations.render.components.RenderedDocumentContent,
    great_expectations.render.components.RenderedGraphContent,
    great_expectations.render.components.RenderedHeaderContent,
    great_expectations.render.components.RenderedMarkdownContent,
    great_expectations.render.components.RenderedSectionContent,
    great_expectations.render.components.RenderedTabsContent,
    great_expectations.render.components.TextContent,
    great_expectations.render.components.ValueListContent
    """

    write_public_api_registry_file()
