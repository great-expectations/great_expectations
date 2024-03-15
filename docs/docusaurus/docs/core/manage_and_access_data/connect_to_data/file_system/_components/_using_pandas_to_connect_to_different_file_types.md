:::info Using pandas to connect to different file types
Great Expectations supports connecting to most types of files that pandas has `read_*` methods for.

Because you will be using pandas to connect to these files, the specific `add_*_asset` methods that will be available to you will be determined by your currently installed version of pandas.

For more information on which pandas `read_*` methods are available to you as `add_*_asset` methods, please reference the official pandas Input/Output documentation for the version of pandas that you have installed.

In the GX Python API, `add_*_asset` methods will require the same parameters as the corresponding pandas `read_*` method, with one caveat: In GX, you will also be required to provide a value for an `asset_name` parameter.
:::