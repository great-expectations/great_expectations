"""

{
    "dataset_name" : "",
    "expectations" : [{
        "expectation_type" : "",
        "kwargs" : {
            ...
        }
    }]
}

"""

import pandas as pd
import numpy as np

from util import DotDict

class DataSet(object):

    def __init__(self, *args, **kwargs):
        super(DataSet, self).__init__(*args, **kwargs)
        self.initialize_expectations()

    def initialize_expectations(self, config=None, name=None):

        if config != None:
            #!!! Should validate the incoming config with jsonschema here

            # Copy the original so that we don't overwrite it by accident
            self._expectation_config = copy.deepcopy(config)

        else:
            self._expectation_config = DotDict({
                "dataset_name" : None,
                "expectations" : []
            })

            for col in self.columns:
                self._expectation_config.expectations.append({
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : {
                        "column" : col
                    }
                })

        self._expectation_config.dataset_name = name

    def append_expectation(self, expectation_config):
        expectation_type = expectation_config['expectation_type']

        #Drop existing expectations with the same expectation_type.
        #!!! This is good default behavior, but
        #!!!    it needs to be documented, and
        #!!!    we need to provide syntax to override it.
        self._expectation_config.expectations = filter(
            lambda exp: exp['expectation_type'] != expectation_type,
            self._expectation_config.expectations 
        )

        self._expectation_config.expectations.append(expectation_config)

    @staticmethod
    def expectation(func):

        def wrapper(self, *args, **kwargs):

            #Get the name of the method
            method_name = func.__name__

            #Fetch argument names
            method_arg_names = inspect.getargspec(func)[0][1:]

            #Construct the expectation_config object
            expectation_config = dict(
                zip(method_arg_names, args)+\
                kwargs.items()
            )

            #Add the expectation_method key
            expectation_config['expectation_type'] = method_name

            #Append the expectation to the config.
            self.append_expectation(expectation_config)

            #Finally, execute the expectation method itself
            return func(self, *args, **kwargs)

        return wrapper


    @staticmethod
    def column_expectation(func):

        def wrapper(self, column, *args, **kwargs):
            #Get the name of the method
            method_name = func.__name__

            #Fetch argument names
            method_arg_names = inspect.getargspec(func)[0][2:]

            if 'verbose' in kwargs:
                verbose = kwargs['verbose']
                del kwargs['verbose']
            else:
                verbose = False

            #Construct the expectation_config object
            expectation_config = {
                "expectation_type" : method_name,
                "kwargs" : dict(
                    zip(method_arg_names, args)+\
                    kwargs.items()
                )
            }

            #Append the expectation to the table config.
            #!!! Add logic to remove duplicate expectations (unless overridden)
            # self.append_expectation(expectation_config)
            self.append_expectation(expectation_config)

            #Finally, execute the expectation method itself
            pass_expectation, exceptions_list = func(self, column, *args, **kwargs)

            if verbose:
                if not pass_expectation:
                    if exceptions_list == None:
                        print 'Sorry,', method_name, 'does not return an exceptions_list.'
                    else:
                        print
                        print len(exceptions_list)*1./self.shape[0], '% exceptions, including:'
                        print exceptions_list[:20]

            return pass_expectation, exceptions_list

        return wrapper

    def get_config(self):
        raise NotImplementedError

    def save_expectations(self):
        raise NotImplementedError

    def validate(self):
        raise NotImplementedError

