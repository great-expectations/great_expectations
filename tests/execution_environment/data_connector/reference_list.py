# <WILL> TODO : move this somewhere better
class ReferenceListForTests(object):
    def __init__(self):
        self.ref_list = ['Hydrogen', 'Helium', 'Lithium', 'Beryllium', 'Boron', 'Carbon', 'Nitrogen', 'Oxygen', 'Fluorine', 'Neon', 'Sodium', 'Magnesium', 'Aluminum', 'Silicon', 'Phosphorus', 'Sulfur', 'Chlorine', 'Argon', 'Potassium', 'Calcium', 'Scandium', 'Titanium', 'Vanadium', 'Chromium', 'Manganese', 'Iron', 'Cobalt', 'Nickel', 'Copper', 'Zinc', 'Gallium', 'Germanium', 'Arsenic', 'Selenium', 'Bromine', 'Krypton', 'Rubidium', 'Strontium', 'Yttrium', 'Zirconium', 'Niobium', 'Molybdenum', 'Technetium', 'Ruthenium', 'Rhodium', 'Palladium', 'Silver', 'Cadmium', 'Indium', 'Tin', 'Antimony', 'Tellurium', 'Iodine', 'Xenon', 'Cesium', 'Barium', 'Lanthanum', 'Cerium', 'Praseodymium', 'Neodymium', 'Promethium', 'Samarium', 'Europium', 'Gadolinium', 'Terbium', 'Dysprosium', 'Holmium', 'Erbium', 'Thulium', 'Ytterbium', 'Lutetium', 'Hafnium', 'Tantalum', 'Tungsten', 'Rhenium', 'Osmium', 'Iridium', 'Platinum', 'Gold', 'Mercury', 'Thallium', 'Lead', 'Bismuth', 'Polonium', 'Astatine', 'Radon', 'Francium', 'Radium', 'Actinium', 'Thorium', 'Protactinium', 'Uranium', 'Neptunium', 'Plutonium', 'Americium', 'Curium', 'Berkelium', 'Californium', 'Einsteinium', 'Fermium', 'Mendelevium', 'Nobelium', 'Lawrencium', 'Rutherfordium', 'Dubnium', 'Seaborgium', 'Bohrium', 'Hassium', 'Meitnerium', 'Darmstadtium', 'Roentgenium', 'Copernicium', 'Nihomium', 'Flerovium', 'Moscovium', 'Livermorium', 'Tennessine', 'Oganesson']



from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext

#
class ReferenceConfigsForTest(object):
    def __init__(self):
        test_action_list = []
        test_json_s3_bucket = "dev_s3_test_bucket"
        test_html_docs_s3_bucket = "dev_html_docs_s3_bucket"
        test_site_name = "dev_sites"
        project_config = DataContextConfig(
            config_version=2,
            execution_environments={
                'my_test_execution_environment': {
                    'class_name': 'ExecutionEnvironment',
                    'execution_engine': {
                        'module_name': 'great_expectations.execution_engine',
                        'class_name': 'PandasExecutionEngine',
                        'caching': True,
                        'batch_spec_defaults': {}
                    },
                    'data_connectors': {
                        "general_pipeline_data_connector": {
                            'module_name': 'great_expectations.execution_environment.data_connector',
                            'class_name': 'PipelineDataConnector',
                            'assets': {
                                'test_asset_1': {
                                    'config_params': {
                                        'partition_name': 'check_dataframe',
                                    },
                                }
                            }
                        },
                        "general_filesystem_data_connector": {
                            'module_name': 'great_expectations.execution_environment.data_connector',
                            'class_name': 'FilesDataConnector',
                            'config_params': {
                                'base_directory': '/my_dir',
                                'glob_directive': '*',
                            },
                            'partitioners': {
                                'my_standard_regex_partitioner': {
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
                                },
                                'my_standard_pipeline_partitioner':{
                                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
                                    'class_name': 'PipelinePartitioner',
                                    'allow_multipart_partitions': False,
                                    'sorters': [],
                                    'config_params': {}

                                }
                            },
                            'default_partitioner': 'my_standard_partitioner',
                            'assets': {
                                'test_asset_0': {
                                    'config_params': {
                                        'glob_directive': 'alex*',
                                    },
                                    'partitioner': 'my_standard_partitioner',
                                }
                            }
                        }
                    }
                }
            },
            config_variables_file_path=None,
            plugins_directory=None,
            validation_operators={
                'action_list_operator': {
                    'class_name': 'ActionListValidationOperator',
                    'action_list': test_action_list
                }
            },
            stores={
                'expectations': {
                    'class_name': 'ExpectationsStore',
                    'store_backend': {
                        'class_name': 'TupleS3StoreBackend',
                        'bucket': test_json_s3_bucket,
                        'prefix': 'great_expectations/JSON/EXECUTION_ENVIRONMENT_ENGINE/ExpectationSuites'
                    }
                },
                'validations': {
                    'class_name': 'ValidationsStore',
                    'store_backend': {
                        'class_name': 'TupleS3StoreBackend',
                        'bucket': test_json_s3_bucket,
                        'prefix': 'great_expectations/JSON/EXECUTION_ENVIRONMENT_ENGINE/Validations'
                    }
                },
                'evaluation_parameters': {
                    'class_name': 'EvaluationParameterStore'
                }
            },
            expectations_store_name='expectations',
            validations_store_name='validations',
            evaluation_parameter_store_name='evaluation_parameters',
            data_docs_sites={
                test_site_name: {
                    'class_name': 'SiteBuilder',
                    'store_backend': {
                        'class_name': 'TupleS3StoreBackend',
                        'bucket': test_html_docs_s3_bucket,
                        'prefix': 'great_expectations/HTML/EXECUTION_ENVIRONMENT_ENGINE'
                    },
                    'site_index_builder': {
                        'class_name': 'DefaultSiteIndexBuilder',
                        'show_cta_footer': True
                    }
                }
            }
        )
        ge_context = BaseDataContext(project_config=project_config)
        self._ge_context = ge_context
        self._config = project_config

    @property
    def ge_context(self):
        return self._ge_context

    @property
    def ge_config(self):
        return self._config