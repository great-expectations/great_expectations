## Teradata SQL Driver Dialect for SQLAlchemy

This package enables [SQLAlchemy](https://pypi.org/project/SQLAlchemy/) to connect to the Teradata Database.

This package requires 64-bit Python 3.4 or later, and runs on Windows, macOS, and Linux. 32-bit Python is not supported.

For community support, please visit the [Teradata Community forums](https://community.teradata.com/).

For Teradata customer support, please visit [Teradata Access](https://access.teradata.com/).

Copyright 2020 Teradata. All Rights Reserved.

### Table of Contents

* [Installation](#Installation)
* [License](#License)
* [Documentation](#Documentation)
* [Using the Teradata SQL Driver Dialect for SQLAlchemy](#Using)
* [Connection Parameters](#ConnectionParameters)

<a name="Installation"></a>

### Installation

Use pip to install the Teradata SQL Driver Dialect for SQLAlchemy.

Platform       | Command
-------------- | ---
macOS or Linux | `pip install teradatasqlalchemy`
Windows        | `py -3 -m pip install teradatasqlalchemy`

When upgrading to a new version of the Teradata SQL Driver Dialect for SQLAlchemy, you may need to use pip install's `--no-cache-dir` option to force the download of the new version.

Platform       | Command
-------------- | ---
macOS or Linux | `pip install --no-cache-dir -U teradatasqlalchemy`
Windows        | `py -3 -m pip install --no-cache-dir -U teradatasqlalchemy`

<a name="License"></a>

### License

Use of the Teradata SQL Driver Dialect for SQLAlchemy is governed by the *License Agreement for the Teradata SQL Driver Dialect for SQLAlchemy*.

When the Teradata SQL Driver Dialect for SQLAlchemy is installed, the `LICENSE` file is placed in the `teradatasqlalchemy` directory under your Python installation directory.

<a name="Documentation"></a>

### Documentation

When the Teradata SQL Driver Dialect for SQLAlchemy is installed, the `README.md` file is placed in the `teradatasqlalchemy` directory under your Python installation directory. This permits you to view the documentation offline, when you are not connected to the Internet.

The `README.md` file is a plain text file containing the documentation for the Teradata SQL Driver Dialect for SQLAlchemy. While the file can be viewed with any text file viewer or editor, your viewing experience will be best with an editor that understands Markdown format.

<a name="Using"></a>

### Using the Teradata SQL Driver Dialect for SQLAlchemy

Your Python script must import the `sqlalchemy` package in order to use the Teradata SQL Driver Dialect for SQLAlchemy.

    import sqlalchemy

After importing the `sqlalchemy` package, your Python script calls the `sqlalchemy.create_engine` function to open a connection to the Teradata Database.

Specify the Teradata Database hostname as the *host* component of the URL. Note that COP Discovery is not implemented yet.

The URL's *host* component may optionally be followed by a slash and question mark `/?` and the URL's *query* component consisting of connection parameters specified as *key*`=`*value* pairs separated by ampersand `&` characters.

The username and password may be specified as a *host* prefix, or as connection URL parameters.

Username and password specified as a *host* prefix:

    eng = sqlalchemy.create_engine('teradatasql://guest:please@whomooz')

Username and password specified as connection URL parameters:

    eng = sqlalchemy.create_engine('teradatasql://whomooz/?user=guest&password=please')

Username and password specified as connection URL parameters take precedence over a *host* prefix, if both are specified.

<a name="ConnectionParameters"></a>

### Connection Parameters

The following table lists the connection parameters currently offered by the Teradata SQL Driver Dialect for SQLAlchemy.

Our goal is consistency for the connection parameters offered by the Teradata SQL Driver Dialect for SQLAlchemy and the Teradata JDBC Driver, with respect to connection parameter names and functionality. For comparison, Teradata JDBC Driver connection parameters are [documented here](http://developer.teradata.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BGBHDDGB).

Parameter          | Default   | Type    | Description
------------------ | --------- | ------- | ---
`account`          |           | string  | Specifies the Teradata Database account. Equivalent to the Teradata JDBC Driver `ACCOUNT` connection parameter.
`column_name`      | `false`   | boolean | Controls the behavior of cursor `.description` sequence `name` items. Equivalent to the Teradata JDBC Driver `COLUMN_NAME` connection parameter. False specifies that a cursor `.description` sequence `name` item provides the AS-clause name if available, or the column name if available, or the column title. True specifies that a cursor `.description` sequence `name` item provides the column name if available, but has no effect when StatementInfo parcel support is unavailable.
`cop`              | `true`    | boolean | Specifies whether COP Discovery is performed. Equivalent to the Teradata JDBC Driver `COP` connection parameter.
`coplast`          | `false`   | boolean | Specifies how COP Discovery determines the last COP hostname. Equivalent to the Teradata JDBC Driver `COPLAST` connection parameter. When `coplast` is `false` or omitted, or COP Discovery is turned off, then no DNS lookup occurs for the coplast hostname. When `coplast` is `true`, and COP Discovery is turned on, then a DNS lookup occurs for a coplast hostname.
`database`         |           | string  | Specifies the initial database to use after logon, instead of the user's default database. Equivalent to the Teradata JDBC Driver `DATABASE` connection parameter.
`dbs_port`         | `1025`    | integer | Specifies Teradata Database port number. Equivalent to the Teradata JDBC Driver `DBS_PORT` connection parameter.
`encryptdata`      | `false`   | boolean | Controls encryption of data exchanged between the Teradata Database and the Teradata SQL Driver for Python. Equivalent to the Teradata JDBC Driver `ENCRYPTDATA` connection parameter.
`fake_result_sets` | `false`   | boolean | Controls whether a fake result set containing statement metadata precedes each real result set.
`lob_support`      | `true`    | boolean | Controls LOB support. Equivalent to the Teradata JDBC Driver `LOB_SUPPORT` connection parameter.
`log`              | `0`       | integer | Controls debug logging. Somewhat equivalent to the Teradata JDBC Driver `LOG` connection parameter. This parameter's behavior is subject to change in the future. This parameter's value is currently defined as an integer in which the 1-bit governs function and method tracing, the 2-bit governs debug logging, the 4-bit governs transmit and receive message hex dumps, and the 8-bit governs timing. Compose the value by adding together 1, 2, 4, and/or 8.
`logdata`          |           | string  | Specifies extra data for the chosen logon authentication method. Equivalent to the Teradata JDBC Driver `LOGDATA` connection parameter.
`logmech`          | `TD2`     | string  | Specifies the logon authentication method. Equivalent to the Teradata JDBC Driver `LOGMECH` connection parameter. Possible values are `TD2` (the default), `JWT`, `LDAP`, `KRB5` for Kerberos, or `TDNEGO`.
`max_message_body` | `2097000` | integer | Not fully implemented yet and intended for future usage. Equivalent to the Teradata JDBC Driver `MAX_MESSAGE_BODY` connection parameter.
`partition`        | `DBC/SQL` | string  | Specifies the Teradata Database Partition. Equivalent to the Teradata JDBC Driver `PARTITION` connection parameter.
`password`         |           | string  | Specifies the Teradata Database password. Equivalent to the Teradata JDBC Driver `PASSWORD` connection parameter.
`sip_support`      | `true`    | boolean | Controls whether StatementInfo parcel is used. Equivalent to the Teradata JDBC Driver `SIP_SUPPORT` connection parameter.
`teradata_values`  | `true`    | boolean | Controls whether `str` or a more specific Python data type is used for certain result set column value types.
`tmode`            | `DEFAULT` | string  | Specifies the transaction mode. Equivalent to the Teradata JDBC Driver `TMODE` connection parameter. Possible values are `DEFAULT` (the default), `ANSI`, or `TERA`.
`user`             |           | string  | Specifies the Teradata Database username. Equivalent to the Teradata JDBC Driver `USER` connection parameter.
