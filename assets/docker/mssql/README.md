After running `docker compose up -d` in this directory to start the mssql container.

A second service will create the `test_ci` database required for the tests. This service will exit once it has created the database. If the database exists, it will noop. This is normal.

You should now be able to run the tests via `pytest --mssql`

## If your Mac computer has an Apple M1 chip, you might need to 
    
1. specify additional compiler or linker options. For example:

    ```sh
    export LDFLAGS="-L/opt/homebrew/Cellar/unixodbc/[your version]/lib"
    export CPPFLAGS="-I/opt/homebrew/Cellar/unixodbc/[your version]/include"
    ``` 

2. Turn on `activate Rosetta emulation (beta feature)` within docker desktop

    See: https://github.com/microsoft/mssql-docker/issues/668#issuecomment-1420259510

3. Install the ODSBC 17 driver: 

    https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver15

### Trouble shooting for Apple Silicon Macs
    
1. If you are seeing the following error:

    ```
    E   ImportError: dlopen(/Users/phamt/code/work/great_expectations/gx_dev/lib/python3.9/site-packages/pyodbc.cpython-39-darwin.so, 0x0002): symbol not found in flat namespace '_SQLAllocHandle'
    ```

    Reinstall pyodbc:

    ```sh
    python -m pip install --force-reinstall --no-binary :all: pyodbc
    python -c "import pyodbc; print(pyodbc.version)"
    ```

2. If you are getting `Login timeout expired` when using localhost, try setting:

    ```sh
    export GE_TEST_LOCAL_DB_HOSTNAME=127.0.0.1 
    ```

    

