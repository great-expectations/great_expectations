# TODO: <Alex>ALEX</Alex>
# from __future__ import annotations
#
# import logging
# from typing import TYPE_CHECKING, ClassVar, Optional, Set
#
# from great_expectations.experimental.datasources.data_asset.data_connector import (
#     DataConnector,
#     FilesystemDataConnector,
# )
# from great_expectations.experimental.datasources.file_path_data_asset import (
#     _FilePathDataAsset,
# )
#
#
# logger = logging.getLogger(__name__)
#
#
# class _FilesystemDataAsset(_FilePathDataAsset):
#     _EXCLUDE_FROM_READER_OPTIONS: ClassVar[
#         Set[str]
#     ] = _FilePathDataAsset._EXCLUDE_FROM_READER_OPTIONS | {
#         "glob_directive",
#     }
#
#     # Filesystem specific attributes
#     glob_directive: str = "**/*"
#
#     def _build_data_connector(self) -> DataConnector:
#         data_connector: DataConnector = FilesystemDataConnector(
#             datasource_name=self.datasource.name,
#             data_asset_name=self.name,
#             base_directory=self.datasource.base_directory,
#             regex=self.regex,
#             glob_directive=self.glob_directive,
#             data_context_root_directory=self.datasource.data_context_root_directory,
#         )
#         return data_connector
#
#     def _build_test_connection_error_message(self) -> str:
#         return f"""No file at base_directory path "{self.datasource.base_directory.resolve()}" matched regular expressions pattern "{self.regex.pattern}" and/or glob_directive "{self.glob_directive}" for DataAsset "{self.name}"."""
# TODO: <Alex>ALEX</Alex>
