.. _migrating_versions:


Migrating between Versions
===========================

Great Expectations provides a warning when the currently-installed version is different from the version stored in the
expectation suite.

Since expectation semantics are usually consistent across versions, there is little change required when upgrading
great expectations, with some exceptions noted here.

In version 0.7, GE introduced several new features, and significantly changed the way DataContext objects work:

 - A :ref:`data_context` object manages access to expectation suites and other configuration in addition to data assets.
   It provides a flexible but opinionated structure for creating and storing configuration and expectations in version
   control.

 - When upgrading from prior versions, the new :ref:`datasource` objects provide the same functionality that compute-
   environment-specific data context objects provided before, but with significantly more flexibility.

 - The term "autoinspect" is no longer used directly, having been replaced by a much more flexible :ref:`profiling`
   feature.