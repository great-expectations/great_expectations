import yaml
import glob

def read_config_from_file(config_filename):
    with open(config_filename, 'r') as f_:
        config = f_.read()
    
    return config


def test_add_store_immediately_adds_to_config(empty_data_context):
    context = empty_data_context
    config_filename = context.root_directory+"/great_expectations.yml"

    assert not "my_new_store" in read_config_from_file(config_filename)
    context.add_store(
        "my_new_store", {
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        }
    )
    assert "my_new_store" in read_config_from_file(config_filename)




# # Methods that might be related to loading and saving configs
#     def validate_config(cls, project_config):
#     def __init__(self, project_config, context_root_dir=None, runtime_environment=None):
#     def test_yaml_config(
#                     config_defaults={
#                     config, runtime_environment={}, config_defaults={}
#     def _get_global_config_value(
#     def _apply_global_config_overrides(self):
#     def _project_config_with_variables_substituted(self):

#     # These two don't touch configs directly, but they're likely to be good examples of the happy path
#     def _build_store_from_config(self, store_name, store_config):
#                 config_defaults={"module_name": module_name},
#     def _init_stores(self, store_configs):

#     # This is probably a good example of the unhappy path. :) :(
#     def _build_datasource_from_config(self, name, config):
#             config_defaults={"module_name": module_name},

#     def _load_config_variables_file(self):
#         """Get all config variables from the default location."""
#                 defined_path = substitute_config_variable(
#                 if not os.path.isabs(defined_path):
#                 var_path = os.path.join(root_directory, defined_path)
#     def get_config_with_variables_substituted(self, config=None):
#     def escape_all_config_variables(
#     def save_config_variable(self, config_variable_name, value):
#     def get_config(self):

#     def config_variables_yml_exist(cls, ge_dir):
#     def write_config_variables_template_to_disk(cls, uncommitted_dir):
#     def write_project_template_to_disk(cls, ge_dir, usage_statistics_enabled=True):
#     _save_project_config
