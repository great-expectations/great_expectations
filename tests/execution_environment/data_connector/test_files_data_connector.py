
partitioner_config = {'partitioners': {
                                'my_standard_partitioner': {
                                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
                                    'class_name': 'RegexPartitioner',
                                    'config_params': {
                                        'regex': {
                                            'pattern': r'.+\/(.+)_(.+)_(.+)\.csv',
                                            'group_names': [
                                                'name',
                                                'timestamp',
                                                'price'
                                            ]
                                        },
                                    },
                                    'allow_multipart_partitions': False,
                                    'sorters': [
                                        {
                                            'name': 'name',
                                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                                            'class_name': 'LexicographicSorter',
                                            'orderby': 'asc',
                                        },
                                        {
                                            'name': 'timestamp',
                                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                                            'class_name': 'DateTimeSorter',
                                            'orderby': 'desc',
                                            'config_params': {
                                                'datetime_format': '%Y%m%d',
                                            }
                                        },
                                        {
                                            'name': 'price',
                                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                                            'class_name': 'NumericSorter',
                                            'orderby': 'desc',
                                        },
                                    ]
                                }
                            }
        }
