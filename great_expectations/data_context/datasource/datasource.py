import os
from ruamel.yaml import YAML
import copy

import logging

logger = logging.getLogger(__name__)
yaml = YAML()

class Datasource(object):

    @classmethod
    def from_configuration(cls, **kwargs):
        return cls(**kwargs)

    def __init__(self, name, type_, data_context=None, generators=None):
        self._data_context = data_context
        self._name = name
        self._generators = {}
        if generators is None:
            generators = {}
        self._datasource_config = {
            "type": type_,
            "generators": generators
        }

        extra_config = self._load_datasource_config()
        self._datasource_config.update(extra_config)
        
    def _build_generators(self):
        for generator in self._datasource_config["generators"].keys():
            self.get_generator(generator)

    def _load_datasource_config(self):
        if self._data_context is None:
            # Setup is done; no additional config to read
            return {}
        try:
            config_path = os.path.join(self._data_context.context_root_directory, "great_expectations/datasources", self._name, "config.yml")
            with open(config_path, "r") as data:
                extra_config = yaml.load(data) or {}
            logger.info("Loading config from %s" % str(config_path))
            return extra_config
        except FileNotFoundError:
            logger.debug("No additional config file found.")
            return {}

    def get_credentials(self, profile_name):
        if self._data_context is not None:
            return self._data_context.get_profile_credentials(profile_name)
        return None

    def get_config(self):
        if self._data_context is not None:
            self._save_config()
        return self._datasource_config

    def _save_config(self):
        if self._data_context is not None:
            base_config = copy.deepcopy(self._datasource_config)
            if "config_file" in base_config:
                config_filepath = os.path.join(self._data_context.context_root_directory, base_config.pop["config_file"])
            else:
                config_filepath = os.path.join(self._data_context.context_root_directory, "great_expectations/datasources", self._name, "config.yml")
        else:
            logger.warning("Unable to save config with no data context attached.")

        os.makedirs(os.path.dirname(config_filepath), exist_ok=True)
        with open(config_filepath, "w") as data_file:
                yaml.dump(self._datasource_config, data_file)

    def add_generator(self, name, type_, **kwargs):
        data_asset_generator_class = self._get_generator_class(type_)
        generator = data_asset_generator_class(name=name, datasource=self, **kwargs)
        self._generators[name] = generator
        if not "generators" in self._datasource_config:
            self._datasource_config["generators"] = {}
        self._datasource_config["generators"][name] = generator.get_config()
        if self._data_context is not None:
            self._save_config()
        return generator

    def get_generator(self, generator_name="default"):
        """Get the (named) generator from a datasource)
        """     
        if generator_name in self._generators:
            return self._generators[generator_name]
        elif generator_name in self._datasource_config["generators"]:
            generator_config = copy.deepcopy(self._datasource_config["generators"][generator_name])
        elif len(self._datasource_config["generators"]) == 1:
            # If there's only one generator, we will use it by default
            generator_name = list(self._datasource_config["generators"])[0]
            generator_config = copy.deepcopy(self._datasource_config["generators"][generator_name])
        else:
            raise ValueError(f"Unable to load generator %s -- no configuration found or invalid configuration." % generator_name)
        type_ = generator_config.pop("type")
        generator_class = self._get_generator_class(type_)
        generator = generator_class(name=generator_name, datasource=self, **generator_config)
        self._generators[generator_name] = generator
        return generator

    def get_data_asset(self, data_asset_name, batch_kwargs=None, **kwargs):
        if batch_kwargs is None:
            generator = self.get_generator()
            if generator is not None:
                batch_kwargs = generator.yield_batch_kwargs(data_asset_name)
            else:
                raise ValueError("No generator or batch_kwargs available to provide a dataset.")

        if self._data_context is not None:
            expectations_config = self._data_context.get_expectations_config(data_asset_name, batch_kwargs)
        else:
            expectations_config = None

        return self._get_data_asset(data_asset_name, batch_kwargs, expectations_config, **kwargs)

    
    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config, **kwargs):
        raise NotImplementedError

    def _get_generator_class(self, type_):
        raise NotImplementedError

    def list_data_asset_names(self, generator_name=None):
        generator = self.get_generator(generator_name)
        return generator.list_data_asset_names()

    def build_batch_kwargs(self, **kwargs):
        raise NotImplementedError