from .store import (
    InMemoryStore,
    FilesystemStore,
)

class DataAssetSnapshotStore(FilesystemStore):
    pass


class Holder(object):

            data_asset_snapshot_store = self._project_config["data_asset_snapshot_store"]
            if isinstance(data_asset, PandasDataset):
                if isinstance(data_asset_snapshot_store, dict) and "filesystem" in data_asset_snapshot_store:
                    logger.info("Storing dataset to file")
                    # directory = os.path.join(
                    #     self.root_directory,
                    #     data_asset_snapshot_store["filesystem"]["base_directory"],
                    #     run_id
                    # )
                    filepath = self._get_normalized_data_asset_name_filepath(
                        normalized_data_asset_name,
                        expectation_suite_name,
                        base_path=os.path.join(
                            self.root_directory,
                            data_asset_snapshot_store["filesystem"]["base_directory"],
                            run_id
                        ),
                        file_extension=".csv.gz"
                    )
                    directory, filename = os.path.split(filepath)
                    safe_mmkdir(directory)
                    data_asset.to_csv(
                        filepath,
                        # self._get_normalized_data_asset_name_filepath(
                        #     normalized_data_asset_name,
                        #     expectation_suite_name,
                        #     base_path=os.path.join(
                        #         self.root_directory,
                        #         data_asset_snapshot_store["filesystem"]["base_directory"],
                        #         run_id
                        #     ),
                        #     file_extension=".csv.gz"
                        # ),
                        compression="gzip"
                    )

                if isinstance(data_asset_snapshot_store, dict) and "s3" in data_asset_snapshot_store:
                    bucket = data_asset_snapshot_store["s3"]["bucket"]
                    key_prefix = data_asset_snapshot_store["s3"]["key_prefix"]
                    key = os.path.join(
                        key_prefix,
                        "validations/{run_id}/{data_asset_name}.csv.gz".format(
                            run_id=run_id,
                            data_asset_name=self._get_normalized_data_asset_name_filepath(
                                normalized_data_asset_name,
                                expectation_suite_name,
                                base_path="",
                                file_extension=".csv.gz"
                            )
                        )
                    )
                    validation_results["meta"]["data_asset_snapshot"] = "s3://{bucket}/{key}".format(
                        bucket=bucket,
                        key=key)

                    try:
                        import boto3
                        s3 = boto3.resource('s3')
                        result_s3 = s3.Object(bucket, key)
                        result_s3.put(Body=data_asset.to_csv(compression="gzip").encode('utf-8'))
                    except ImportError:
                        logger.error("Error importing boto3 for AWS support. Unable to save to result store.")
                    except Exception:
                        raise
            else:
                logger.warning(
                    "Unable to save data_asset of type: %s. Only PandasDataset is supported." % type(data_asset))

