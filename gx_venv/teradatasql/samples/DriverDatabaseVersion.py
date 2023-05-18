# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to obtain the version numbers for the
# Teradata SQL Driver for Python and the Teradata Database.

import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        cur.execute ('{fn teradata_nativesql}Driver version {fn teradata_driver_version}  Database version {fn teradata_database_version}')
        print (cur.fetchone () [0])
