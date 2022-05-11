
import random
import uuid
from typing import Union
from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.types.resource_identifiers import ConfigurationIdentifier, GeCloudIdentifier
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig

class ProfilerStore(ConfigurationStore):
    '\n    A ProfilerStore manages Profilers for the DataContext.\n    '
    _configuration_class = RuleBasedProfilerConfig

    def serialization_self_check(self, pretty_print: bool) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Fufills the abstract method defined by the parent class.\n        See `ConfigurationStore` for more details.\n        '
        test_profiler_name = f"profiler_{''.join([random.choice(list('0123456789ABCDEF')) for _ in range(20)])}"
        test_profiler_configuration = RuleBasedProfilerConfig(name=test_profiler_name, config_version=1.0, rules={})
        test_key: Union[(GeCloudIdentifier, ConfigurationIdentifier)]
        if self.ge_cloud_mode:
            test_key = self.key_class(resource_type='contract', ge_cloud_id=str(uuid.uuid4()))
        else:
            test_key = self.key_class(configuration_key=test_profiler_name)
        if pretty_print:
            print(f'Attempting to add a new test key {test_key} to Profiler store...')
        self.set(key=test_key, value=test_profiler_configuration)
        if pretty_print:
            print(f'''	Test key {test_key} successfully added to Profiler store.
''')
            print(f'Attempting to retrieve the test value associated with key {test_key} from Profiler store...')
        test_value = self.get(key=test_key)
        if pretty_print:
            print(f'''	Test value successfully retrieved from Profiler store: {test_value}
''')
            print(f'Cleaning up test key {test_key} and value from Profiler store...')
        test_value = self.remove_key(key=test_key)
        if pretty_print:
            print(f'''	Test key and value successfully removed from Profiler store: {test_value}
''')
