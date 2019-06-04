import os

import pandas as pd
import json
import logging
import uuid
import datetime
import requests
import errno

import great_expectations.dataset as dataset

logger = logging.getLogger(__name__)

def _convert_to_dataset_class(df, dataset_class, expectations_config=None, autoinspect_func=None):
    """
    Convert a (pandas) dataframe to a great_expectations dataset, with (optional) expectations_config
    """
    if expectations_config is not None:
        # Create a dataset of the new class type, and manually initialize expectations according to the provided configuration
        new_df = dataset_class.from_dataset(df)
        new_df._initialize_expectations(expectations_config)
    else:
        # Instantiate the new Dataset with default expectations
        new_df = dataset_class.from_dataset(df)
        if autoinspect_func is not None:
            new_df.autoinspect(autoinspect_func)

    return new_df


def read_csv(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    autoinspect_func=None,
    *args, **kwargs
):
    df = pd.read_csv(filename, *args, **kwargs)
    df = _convert_to_dataset_class(
        df, dataset_class, expectations_config, autoinspect_func)
    return df


def read_json(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    accessor_func=None,
    autoinspect_func=None,
    *args, **kwargs
):
    if accessor_func != None:
        json_obj = json.load(open(filename, 'rb'))
        json_obj = accessor_func(json_obj)
        df = pd.read_json(json.dumps(json_obj), *args, **kwargs)

    else:
        df = pd.read_json(filename, *args, **kwargs)

    df = _convert_to_dataset_class(
        df, dataset_class, expectations_config, autoinspect_func)
    return df


def read_excel(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    autoinspect_func=None,
    *args, **kwargs
):
    """Read a file using Pandas read_excel and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectations_config (string): path to great_expectations config file

    Returns:
        great_expectations dataset or ordered dict of great_expectations datasets,
        if multiple worksheets are imported
    """
    df = pd.read_excel(filename, *args, **kwargs)
    if isinstance(df, dict):
        for key in df:
            df[key] = _convert_to_dataset_class(
                df[key], dataset_class, expectations_config, autoinspect_func)
    else:
        df = _convert_to_dataset_class(
            df, dataset_class, expectations_config, autoinspect_func)
    return df


def read_table(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    autoinspect_func=None,
    *args, **kwargs
):
    """Read a file using Pandas read_table and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectations_config (string): path to great_expectations config file

    Returns:
        great_expectations dataset
    """
    df = pd.read_table(filename, *args, **kwargs)
    df = _convert_to_dataset_class(
        df, dataset_class, expectations_config, autoinspect_func)
    return df


def read_parquet(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    autoinspect_func=None,
    *args, **kwargs
):
    """Read a file using Pandas read_parquet and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectations_config (string): path to great_expectations config file

    Returns:
        great_expectations dataset
    """
    df = pd.read_parquet(filename, *args, **kwargs)
    df = _convert_to_dataset_class(
        df, dataset_class, expectations_config, autoinspect_func)
    return df


def from_pandas(pandas_df, 
                dataset_class=dataset.pandas_dataset.PandasDataset,
                expectations_config=None, 
                autoinspect_func=None
):
    """Read a Pandas data frame and return a great_expectations dataset.

    Args:
        pandas_df (Pandas df): Pandas data frame
        dataset_class (Dataset class) = dataset.pandas_dataset.PandasDataset:
            class to which to convert resulting Pandas df
        expectations_config (string) = None: path to great_expectations config file
        autoinspect_func (function) = None: The autoinspection function that should 
            be run on the dataset to establish baseline expectations.

    Returns:
        great_expectations dataset
    """
    return _convert_to_dataset_class(
        pandas_df,
        dataset_class,
        expectations_config,
        autoinspect_func
    )

def validate(data_asset, expectations_config=None, data_asset_name=None, data_context=None, data_asset_type=None, *args, **kwargs):
    """Validate the provided data asset using the provided config"""
    if expectations_config is None and data_context is None:
        raise ValueError("Either an expectations config or a DataContext is required for validation.")

    if expectations_config is None:
        logger.info("Using expectations config from DataContext.")
        expectations_config = data_context.get_expectations_config(data_asset_name)
    else:
        if data_asset_name in expectations_config:
            logger.info("Using expectations config with name %s" % expectations_config["data_asset_name"])
        else:
            logger.info("Using expectations config with no data_asset_name")

    # If the object is already a Dataset type, then this is purely a convenience method
    # and no conversion is needed
    if isinstance(data_asset, dataset.Dataset) and data_asset_type is None:
        return data_asset.validate(expectations_config=expectations_config, data_context=data_context, *args, **kwargs)
    elif data_asset_type is None:
        # Guess the GE data_asset_type based on the type of the data_asset
        if isinstance(data_asset, pd.DataFrame):
            data_asset_type = dataset.PandasDataset
        # Add other data_asset_type conditions here as needed

    # Otherwise, we will convert for the user to a subclass of the
    # existing class to enable new expectations, but only for datasets
    if not isinstance(data_asset, (dataset.Dataset, pd.DataFrame)):
        raise ValueError("The validate util method only supports dataset validations, including custom subclasses. For other data asset types, use the object's own validate method.")

    if not issubclass(type(data_asset), data_asset_type):
        if isinstance(data_asset, (pd.DataFrame)) and issubclass(data_asset_type, dataset.PandasDataset):
            pass # This is a special type of allowed coercion
        else:
            raise ValueError("The validate util method only supports validation for subtypes of the provided data_asset_type.")

    data_asset_ = _convert_to_dataset_class(data_asset, data_asset_type, expectations_config)
    return data_asset_.validate(*args, data_context=data_context, **kwargs)


def build_slack_notification_request(validation_json=None):
    # Defaults
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), "%x %X")
    status = "Failed :x:"
    run_id = None
    data_asset_name = "no_name_provided_" + str(uuid.uuid4())
    title_block = {
               "type": "section",
               "text": {
                   "type": "mrkdwn",
                   "text": "No validation occurred. Please ensure you passed a validation_json.",
               },
           }

    query = {"blocks": [title_block]}

    if validation_json:
        if "meta" in validation_json and "data_asset_name" in validation_json["meta"]:
            data_asset_name = validation_json["meta"]["data_asset_name"]

        n_checks_succeeded = validation_json["statistics"]["successful_expectations"]
        n_checks = validation_json["statistics"]["evaluated_expectations"]
        run_id = validation_json["meta"].get("run_id", None)
        check_details_text = "{} of {} expectations were met\n\n".format(n_checks_succeeded, n_checks)

        if validation_json["success"]:
            status = "Success :tada:"

        query["blocks"][0]["text"]["text"] = "*Validated dataset:* `{}`\n*Status: {}*\n{}".format(data_asset_name, status, check_details_text)

        if "result_reference" in validation_json["meta"]:
            report_element = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation Report*: {}".format(validation_json["meta"]["result_reference"])},
            }
            query["blocks"].append(report_element)

        if "dataset_reference" in validation_json["meta"]:
            dataset_element = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation Dataset*: {}".format(validation_json["meta"]["dataset_reference"])
                },
            }
            query["blocks"].append(dataset_element)

    footer_section = {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "Great Expectations run id {} ran at {}".format(run_id, timestamp),
            }
        ],
    }
    query["blocks"].append(footer_section)
    return query


def get_slack_callback(webhook):
    def send_slack_notification(validation_json=None):
        """
            Post a slack notification.
        """
        session = requests.Session()
        query = build_slack_notification_request(validation_json)

        try:
            response = session.post(url=webhook, json=query)
        except requests.ConnectionError:
            logger.warning(
                'Failed to connect to Slack webhook at {url} '
                'after {max_retries} retries.'.format(
                    url=webhook, max_retries=10))
        except Exception as e:
            logger.error(str(e))
        else:
            if response.status_code != 200:
                logger.warning(
                    'Request to Slack webhook at {url} '
                    'returned error {status_code}: {text}'.format(
                        url=webhook,
                        status_code=response.status_code,
                        text=response.text))
    return send_slack_notification


class DotDict(dict):
    """dot.notation access to dictionary attributes"""

    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __dir__(self):
        return self.keys()

def safe_mmkdir(directory, exist_ok=True): #exist_ok is  always true; it's ignored, but left here to make porting later easier
    """Simple wrapper since exist_ok is not available in python 2"""
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise