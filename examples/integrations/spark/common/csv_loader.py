from pathlib import Path

from pyspark import keyword_only
from pyspark.ml.param.shared import Param
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

class CSVLoader(object):
    """
    Loads a csv file into a dataframe
    """

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 view: str,
                 path_to_csv: str,
                 delimiter=",",
                 limit: int = -1,
                 schema: StructType = None,
                 cache_table: bool = True,
                 has_header: bool = True,
                 infer_schema: bool = False):
        super(TemplateTransformer, self).__init__()

        self.logger = get_logger(__name__)

        self.view = Param(self, "view", "")
        self._setDefault(view=None)

        self.path_to_csv = Param(self, "path_to_csv", "")
        self._setDefault(path_to_csv=None)

        self.delimiter = Param(self, "delimiter", "")
        self._setDefault(delimiter=",")

        self.schema = Param(self, "schema", "")
        self._setDefault(schema=None)

        self.cache_table = Param(self, "cache_table", "")
        self._setDefault(cache_table=True)

        self.has_header = Param(self, "has_header", "")
        self._setDefault(has_header=True)

        self.limit = Param(self, "limit", "")
        self._setDefault(limit=-1)

        self.infer_schema = Param(self, "infer_schema", "")
        self._setDefault(infer_schema=False)

        if not path_to_csv:
            raise ValueError("path_to_csv is empty")

        self.logger.info("passed path_to_csv: " + path_to_csv)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    @keyword_only
    def setParams(self, view: str,
                  path_to_csv: str,
                  delimiter=",",
                  limit: int = -1,
                  schema: StructType = None,
                  cache_table: bool = True,
                  has_header: bool = True,
                  infer_schema: bool = False
                  ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df: DataFrame):
        view = self.getView()
        path_to_csv = self.getPathToCsv()
        schema = self.getSchema()
        cache_table = self.getCacheTable()
        has_header = self.getHasHeader()
        infer_schema = self.getInferSchema()
        limit = self.getLimit()

        if not path_to_csv:
            raise ValueError(f"path_to_csv is empty: {path_to_csv}")

        if path_to_csv.__contains__(":"):
            full_path_to_csv = path_to_csv
        else:
            data_dir: str = Path(__file__).parent.parent.joinpath('./')
            full_path_to_csv: str = f"file://{data_dir.joinpath(path_to_csv)}"

        delimiter = self.getDelimiter()

        self.logger.info(
            f"Loading csv file for view {view}: {full_path_to_csv}, infer_schema: {infer_schema}")

        df2 = df.sql_ctx.read

        if schema:
            df2 = df2.schema(schema)
        elif infer_schema:
            df2 = df2.option("inferSchema", "true")

        # https://docs.databricks.com/spark/latest/data-sources/read-csv.html
        df2 = df2.format("com.databricks.spark.csv") \
            .option("header", "true" if has_header else "false") \
            .option("delimiter", delimiter) \
            .load(full_path_to_csv)

        if limit and limit > -1:
            df2 = df2.limit(limit)

        df2.createOrReplaceTempView(view)

        if cache_table:
            self.logger.info(f"caching table {view}")
            df.sql_ctx.sql(f"CACHE TABLE {view}")

        self.logger.info(
            f"Finished Loading csv file for view {view}: {full_path_to_csv}, infer_schema: {infer_schema}")

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value):
        self._paramMap[self.view] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setPathToCsv(self, value):
        self._paramMap[self.path_to_csv] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPathToCsv(self) -> str:
        return self.getOrDefault(self.path_to_csv)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setDelimiter(self, value):
        self._paramMap[self.delimiter] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDelimiter(self) -> str:
        return self.getOrDefault(self.delimiter)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setSchema(self, value):
        self._paramMap[self.schema] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setCacheTable(self, value):
        self._paramMap[self.cache_table] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getCacheTable(self) -> bool:
        return self.getOrDefault(self.cache_table)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setHasHeader(self, value):
        self._paramMap[self.has_header] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getHasHeader(self) -> bool:
        return self.getOrDefault(self.has_header)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setInferSchema(self, value):
        self._paramMap[self.infer_schema] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getInferSchema(self) -> bool:
        return self.getOrDefault(self.infer_schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLimit(self, value):
        self._paramMap[self.limit] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> str:
        return self.getView()
