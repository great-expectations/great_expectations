import datetime
import functools
import os
import re
import warnings
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import Batch
from great_expectations.core.id_dict import IDDict
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.base_data_asset import BatchSpecPassthrough, DataConnectorQuery, NewBatchRequestBase, NewConfiguredBatchRequest
from great_expectations.datasource.data_connector.util import convert_batch_identifiers_to_data_reference_string_using_regex
from great_expectations.datasource.pandas_reader_data_asset import PandasReaderDataAsset
from great_expectations.marshmallow__shade.fields import Bool
from great_expectations.types import DictDot
from great_expectations.validator.validator import Validator


warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.ParserWarning)


#!!! Factor this out to somewhere nicer
class GxExperimentalWarning(Warning):
    pass

#!!! Rename this
class NewNewNewDatasource:
    pass

#!!! Could this decorator be moved inside PandasReaderDataSource, maybe as a staticmethod?
def _add_gx_args(
    func: Callable = None,
    primary_arg_variable_name: str = None,
    default_use_primary_arg_as_id: bool = None,
    arguments_excluded_from_runtime_parameters: Dict[str, int] = {},
):
    """Decorator to wrap any given pandas.read_* method, and return a Validator.

    This decorator
        1. wraps pandas.read_* methods,
        2. adds some additional input parameters,
        3. creates a RuntimeBatchRequest,
        4. uses it to fetch a Batch, and then
        5. returns a Great Expectations Validator.

    All of the additional input parameters added to the read_* method (primary_arg, id_, use_primary_arg_as_id, expectation_suite) are there so that users can control metadata passed to the RuntimeBatchRequest.

    The decorator itself accepts 3 optional arguments:
    * primary_arg_variable_name : If the read_* method has a first positional argument, then primary_arg_variable_name should match the name of the first argument in the pandas method declaration. For example for read_csv, it's "filepath_or_buffer"
    * default_use_primary_arg_as_id : When the read_* method is called, the first positional argument (aka "primary_arg") might or might make sense to keep as an id_ in BatchRequest.batch_identifiers. For example, a filename passed to read_parquet could be a good id_, but a large JSON object passed to read_jon, probably isn't. default_use_primary_arg_as_id specifies the default value that the PandasReaderDatasource.read_* method will use.
        * True : Default to using the primary_arg as an id_
        * False : Default to not using the primary_arg as an id_
        * None : Use _decide_whether_to_use_variable_as_identifier to make the call on a case-by-case basis
        The user can always override the default behavior when they call read_csv, using the use_primary_arg_as_id parameter.
    * arguments_excluded_from_runtime_parameters : Some read_* methods have parameters that shouldn't be included in runtime_parameters. For example, read_sql includes a sqlalchemy engine connection. Keys in the dictionary are the keyword names of arguments in the pandas read_* method declaration. Values are the positional values of those arguments (again, in the pandas read_* method declaration).
    """

    if func is None:
        return functools.partial(
            _add_gx_args,
            primary_arg_variable_name=primary_arg_variable_name,
            default_use_primary_arg_as_id=default_use_primary_arg_as_id,
            arguments_excluded_from_runtime_parameters=arguments_excluded_from_runtime_parameters,
        )

    def wrapped(
        self,
        primary_arg: Optional[Any] = None,
        *args,
        data_asset_name: Optional[str] = None,
        id_: Optional[str] = None,
        use_primary_arg_as_id: Optional[Bool] = default_use_primary_arg_as_id,
        expectation_suite: Optional[ExpectationSuite] = None,
        timestamp=None,
        **kwargs,
    ):
        #!!! What's the best way to add docstrings for the arguments supplied by the decorator?

        if id_ is not None and use_primary_arg_as_id == True:
            raise ValueError(
                "id_ cannot be specified when use_primary_arg_as_id is also True"
            )

        if id_ is None:
            if use_primary_arg_as_id == None:
                use_primary_arg_as_id = (
                    self._decide_whether_to_use_variable_as_identifier(primary_arg)
                )

            if use_primary_arg_as_id:
                id_ = primary_arg
            else:
                id_ = None

        if timestamp == None:
            timestamp = datetime.datetime.now()

        #!!! Check to ensure serializability of args and kwargs.
        # Non-serializable args and kwargs should be replaced by some token that indicates that they were present, but can't be saved.
        # https://stackoverflow.com/questions/51674222/how-to-make-json-dumps-in-python-ignore-a-non-serializable-field
        # Alternatively, maybe we just make BatchRequests serialization-safe, and worry about it on the output side.

        #!!! This seems like a good place to track usage stats

        if primary_arg_variable_name in kwargs:
            if primary_arg != None:
                raise TypeError(
                    f"{func.__name__}() got multiple values for argument {primary_arg_variable_name}"
                )

            primary_arg = kwargs.pop(primary_arg_variable_name)

        if data_asset_name == None:
            data_asset_name = "DEFAULT_DATA_ASSET"
        else:
            if data_asset_name in self.assets:
                pass
            else:
                self.add_asset(
                    name = data_asset_name,
                    base_directory = "",
                )

        df = func(self, primary_arg, *args, **kwargs)

        args, kwargs = self._remove_excluded_arguments(
            arguments_excluded_from_runtime_parameters,
            args,
            kwargs,
        )

        batch_request = NewConfiguredBatchRequest(
            datasource_name=self.name,
            data_asset_name=data_asset_name,
            data_connector_query=DataConnectorQuery(
                id_= id_,
                timestamp= timestamp,
            ),
            batch_spec_passthrough=BatchSpecPassthrough(
                # method= func,
                # primary_arg= primary_arg,
                args= list(args),
                kwargs= kwargs,
            ),
        )

        batch = Batch(
            data=df,
            batch_request=batch_request,
        )

        validator = Validator(
            execution_engine=self._execution_engine,
            expectation_suite=None,#expectation_suite,
            batches=[batch],
        )

        return validator

        ### Here's my original, hacky implementation for getting a Batch
        # batch = self.get_single_batch_from_batch_request(
        #     batch_request=RuntimeBatchRequest(
        #         datasource_name=self.name,
        #         data_connector_name="runtime_data_connector",
        #         data_asset_name="DEFAULT_DATA_ASSET",
        #         runtime_parameters={
        #             "batch_data": df,
        #             "args": list(args),
        #             "kwargs": kwargs,
        #         },
        #         batch_identifiers={
        #             "id_": id_,
        #             "timestamp": timestamp,
        #         },
        #     )
        # )

        # return self.get_validator(batch_request)

        # batch = Batch(
        #     data=df,
        #     batch_request=NewConfiguredBatchRequest(
        #         datasource_name=self.name,
        #         data_asset_name=data_asset_name,
        #         data_connector_query=DataConnectorQuery(
        #             id_= id_,
        #             timestamp= timestamp,
        #         ),
        #         batch_spec_passthrough=BatchSpecPassthrough(
        #             args= list(args),
        #             kwargs= kwargs,
        #         ),
        #     )
        # )

        # #!!! Returning a Validator goes against the pattern we've used elsewhere for Datasources.
        # # I'm increasingly convinced that this is the right move, rather than returning Batches, which are useless objects.
        # validator = Validator(
        #     execution_engine=self._execution_engine,
        #     expectation_suite=expectation_suite,
        #     batches=[batch],
        # )

        # return validator

    return wrapped


class PandasReaderDatasource(NewNewNewDatasource):
    """
    This class is enables very simple syntax for users who are just getting started with Great Expectations, and have not configured (or learned about) Datasources and DataConnectors. To do so, it provides thin wrapper methods for all 20 of pandas' `read_*` methods.
    
    From a LiteDataContext, it can be invoked as follows: `my_context.datasources.pandas_readers.read_csv`

    Note on limitations:

        This class is based on a RuntimeDataConnector, so no DataConnector configuration is required. This comes with some serious limitations:
            * All dataframes must be read one at a time, with no wildcards within filenames or globbing of filepaths.
            * All dataframes will appear as a single Data Asset in your Data Docs.
            * Can't be profiled with a multibatch Data Assistants
            * ......
            * This DataConnector can't They haven't set up a way to reference this data for recurring validation (e.g. set up a Checkpoint containing a BatchRequest.)
            * No separate concept of Assets


    Notes on pandas read_* methods:

        Almost all of pandas.read_* methods require a single positional argument. In general, they are one of the following:
            1. path-type objects (eg filepaths, URLs) that point to data that can be parsed into a DataFrame,
            2. the contents of a thing to be parsed into a DataFrame (e.g. a JSON string), or
            3. buffer/file-like objects that can be read and parsed into a DataFrame.

        Exceptions are listed here:

            pandas.read_pickle
            pandas.read_table
            pandas.read_csv
            pandas.read_fwf
            pandas.read_excel
            pandas.read_json : The first positional argument is optional. It's not clear from the docs how that works...
            pandas.read_html
            pandas.read_xml
            pandas.read_hdf
            pandas.read_sas
            pandas.read_gbq
            pandas.read_stata
            pandas.read_clipboard : Doesn't have a required positional argument
            pandas.read_feather : Always a path
            pandas.read_parquet : Always a path
            pandas.read_orc : Always a path
            pandas.read_spss : Always a path
            pandas.read_sql_table : Requires a table name (~"always a path") and a connection
            pandas.read_sql_query : Requires a query and a connection
            pandas.read_sql : Requires a query or table name, and a connection

        For case 1, we can use the path-type object itself as the identifier, plus a time stamp.
        For cases 2 and 3, we really have no information about the provenance of the object. Instead, we realy entirely on the time stamp to identify the batch.
    """

    def __init__(
        self,
        name,
    ):
        #!!! Trying this on for size
        # experimental-v0.15.1
        # warnings.warn(
        #     "\n================================================================================\n" \
        #     "PandasReaderDatasource is an experimental feature of Great Expectations\n" \
        #     "You should consider the API to be unstable.\n" \
        #     "If you have questions or feedback, please chime in at\n" \
        #     "https://github.com/great-expectations/great_expectations/discussions/DISCUSSION-ID-GOES-HERE\n" \
        #     "================================================================================\n",
        #     GxExperimentalWarning,
        # )
        self._name = name
        self._assets = DictDot()

        self._execution_engine = instantiate_class_from_config(
            config={
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            runtime_environment={
                "concurrency": None
            },
            config_defaults={
                "module_name": "great_expectations.execution_engine"
            },
        )


        # super().__init__(
        #     name=name,
        #     execution_engine={
        #         "class_name": "PandasExecutionEngine",
        #         "module_name": "great_expectations.execution_engine",
        #     },
        #     data_connectors={
        #         # "runtime_data_connector": {
        #         #     "class_name": "RuntimeDataConnector",
        #         #     "batch_identifiers": [
        #         #         "id_",
        #         #         "timestamp",
        #         #     ],
        #         # },
        #         "configured_data_connector": {
        #             "class_name": "ConfiguredAssetFilesystemDataConnector",
        #             "base_directory":"",
        #             "assets":{
        #                 "DEFAULT_DATA_ASSET":{
        #                     "pattern":"(.*)",
        #                     "group_names":["filename"],
        #                 }
        #             }
        #         }
        #     },
        # )

    def add_asset(
        self,
        name: str,
        base_directory: str,
        method: Optional[str] = "read_csv",
        regex: Optional[str] = "(.*)",
        batch_identifiers: List[str] = ["filename"],
        # check_new_asset: bool = False,
    ) -> PandasReaderDataAsset:

        new_asset = PandasReaderDataAsset(
            datasource=self,
            name=name,
            method=method,
            base_directory=base_directory,
            regex=regex,
            batch_identifiers=batch_identifiers
        )

        self._assets[name] = new_asset

        return new_asset

    def list_asset_names(self) -> List[str]:
        return list(self.assets.keys())

    def get_batch(self, batch_request: NewConfiguredBatchRequest) -> Batch:

        asset = self.assets[batch_request.data_asset_name]

        func = getattr(pd, asset.method)
        filename = convert_batch_identifiers_to_data_reference_string_using_regex(
            batch_identifiers= IDDict(**batch_request.data_connector_query),
            regex_pattern= asset.regex,
            group_names= asset.batch_identifiers,
            # data_asset_name= self.name,
        )
        primary_arg = os.path.join(
            asset.base_directory,
            filename,
        )

        # !!! How do we handle non-serializable elements like `con`?

        args = batch_request.batch_spec_passthrough.get("args", [])
        kwargs = batch_request.batch_spec_passthrough.get("kwargs", {})

        df = func(primary_arg, *args, **kwargs)

        batch = Batch(
            data=df,
            batch_request=batch_request,
        )

        return batch

    def get_validator(self, batch_request: NewConfiguredBatchRequest) -> Batch:
        print(batch_request)
        batch = self.get_batch(batch_request)
        return Validator(
            execution_engine=self._execution_engine,
            expectation_suite=None,#expectation_suite,
            batches=[batch],
        )

    @property
    def assets(self) -> Dict[str, PandasReaderDataAsset]:
        #!!! DotDict is on its way to becoming a deprecated class. Use DictDot instead.
        #!!! Unfortunately, this currently crashes with `TypeError: DictDot() takes no arguments`
        return self._assets

    @property
    def name(self) -> str:
        return self._name

    def _decide_whether_to_use_variable_as_identifier(self, var):
        #!!! This is brittle. Almost certainly needs fleshing out.
        if not isinstance(var, str):
            return False

        # Does the string contain any whitespace?
        return re.search("\s", var) == None

    def _remove_excluded_arguments(
        self,
        arguments_excluded_from_runtime_parameters: Dict[str, int],
        args: List[Any],
        kwargs: Dict[str, Any],
    ) -> Tuple[List[Any], Dict[str, Any]]:
        remove_indices = []
        for arg_name, index in arguments_excluded_from_runtime_parameters.items():
            kwargs.pop(arg_name, None)
            remove_indices.append(index - 1)

        args = [i for j, i in enumerate(args) if j not in remove_indices]

        return args, kwargs

    #!!! What's the best way to add docstrings to these methods?
    @_add_gx_args(primary_arg_variable_name="filepath_or_buffer")
    def read_csv(self, primary_arg, *args, **kwargs):
        return pd.read_csv(primary_arg, *args, **kwargs)

    @_add_gx_args(
        primary_arg_variable_name="path_or_buf", default_use_primary_arg_as_id=False
    )
    def read_json(self, primary_arg, *args, **kwargs):
        return pd.read_json(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="filepath_or_buffer")
    def read_table(self, filepath_or_buffer, *args, **kwargs):
        return pd.read_table(filepath_or_buffer, *args, **kwargs)

    @_add_gx_args(default_use_primary_arg_as_id=False)
    def read_clipboard(self, *args, **kwargs):
        return pd.read_clipboard(*args, **kwargs)

    #!!! Wrong primary_arg_variable_name, I think. Does this method even have one?
    @_add_gx_args(primary_arg_variable_name="filepath_or_buffer")
    def from_dataframe(self, primary_arg, *args, **kwargs):
        def no_op(df):
            return df

        return no_op(primary_arg)

    ### These three methods take a connection as their second argument.

    @_add_gx_args(
        primary_arg_variable_name="table_name",
        default_use_primary_arg_as_id=True,
        arguments_excluded_from_runtime_parameters={"con": 1},
    )
    def read_sql_table(self, primary_arg, *args, **kwargs):
        return pd.read_sql_table(primary_arg, *args, **kwargs)

    @_add_gx_args(
        primary_arg_variable_name="sql",
        default_use_primary_arg_as_id=False,
        arguments_excluded_from_runtime_parameters={"con": 1},
    )
    def read_sql_query(self, primary_arg, *args, **kwargs):
        return pd.read_sql_query(primary_arg, *args, **kwargs)

    @_add_gx_args(
        primary_arg_variable_name="sql",
        arguments_excluded_from_runtime_parameters={"con": 1},
    )
    def read_sql(self, primary_arg, *args, **kwargs):
        return pd.read_sql(primary_arg, *args, **kwargs)

    #!!! Methods below this line aren't yet tested

    @_add_gx_args(primary_arg_variable_name="filepath_or_buffer")
    def read_pickle(self, primary_arg, *args, **kwargs):
        return pd.read_pickle(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="filepath_or_buffer")
    def read_fwf(self, primary_arg, *args, **kwargs):
        return pd.read_fwf(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="io")
    def read_excel(self, primary_arg, *args, **kwargs):
        return pd.read_excel(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="io")
    def read_html(self, primary_arg, *args, **kwargs):
        return pd.read_html(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="path_or_buffer")
    def read_xml(self, primary_arg, *args, **kwargs):
        return pd.read_xml(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="path_or_buf")
    def read_hdf(self, primary_arg, *args, **kwargs):
        return pd.read_hdf(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="filepath_or_buffer")
    def read_sas(self, primary_arg, *args, **kwargs):
        return pd.read_sas(primary_arg, *args, **kwargs)

    @_add_gx_args(
        primary_arg_variable_name="query", default_use_primary_arg_as_id=False
    )
    def read_gbq(self, primary_arg, *args, **kwargs):
        return pd.read_gbq(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="filepath_or_buffer")
    def read_stata(self, primary_arg, *args, **kwargs):
        return pd.read_stata(primary_arg, *args, **kwargs)

    ### These next methods always take a path as their primary argument (buffers not allowed).

    @_add_gx_args(primary_arg_variable_name="path", default_use_primary_arg_as_id=True)
    def read_feather(self, primary_arg, *args, **kwargs):
        return pd.read_feather(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="path", default_use_primary_arg_as_id=True)
    def read_parquet(self, primary_arg, *args, **kwargs):
        return pd.read_parquet(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="path", default_use_primary_arg_as_id=True)
    def read_orc(self, primary_arg, *args, **kwargs):
        return pd.read_orc(primary_arg, *args, **kwargs)

    @_add_gx_args(primary_arg_variable_name="path", default_use_primary_arg_as_id=True)
    def read_spss(self, primary_arg, *args, **kwargs):
        return pd.read_spss(primary_arg, *args, **kwargs)




