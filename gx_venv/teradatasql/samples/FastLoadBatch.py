# Copyright 2019 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to FastLoad batches of rows.

import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        sTableName = "FastLoadBatch"
        try:
            sRequest = "DROP TABLE " + sTableName
            print (sRequest)
            cur.execute (sRequest)
        except Exception as ex:
            print ("Ignoring", str (ex).split ("\n") [0])

        sRequest = "CREATE TABLE " + sTableName + " (c1 INTEGER NOT NULL, c2 VARCHAR(10))"
        print (sRequest)
        cur.execute (sRequest)

        try:
            sRequest = "{fn teradata_nativesql}{fn teradata_autocommit_off}"
            print (sRequest)
            cur.execute (sRequest)

            aaoValues = [
                [1, # c1 INTEGER NOT NULL
                None, # c2 VARCHAR
                ],
                [2, # c1 INTEGER NOT NULL
                "abc", # c2 VARCHAR
                ],
                [3, # c1 INTEGER NOT NULL
                "def", # c2 VARCHAR
                ],
            ]

            sInsert = "{fn teradata_try_fastload}INSERT INTO " + sTableName + " (?, ?)"
            print (sInsert)
            cur.execute (sInsert, aaoValues)

            # obtain the warnings and errors for transmitting the data to the database -- the acquisition phase

            sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            sRequest = "{fn teradata_nativesql}{fn teradata_logon_sequence_number}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            aaoValues = [
                [4, # c1 INTEGER NOT NULL
                "mno", # c2 VARCHAR
                ],
                [5, # c1 INTEGER NOT NULL
                None, # c2 VARCHAR
                ],
                [6, # c1 INTEGER NOT NULL
                "pqr", # c2 VARCHAR
                ],
            ]

            print (sInsert)
            cur.execute (sInsert, aaoValues)

            # obtain the warnings and errors for transmitting the data to the database -- the acquisition phase

            sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            aaoValues = [
                [7, # c1 INTEGER NOT NULL
                "uvw", # c2 VARCHAR
                ],
                [8, # c1 INTEGER NOT NULL
                "xyz", # c2 VARCHAR
                ],
                [9, # c1 INTEGER NOT NULL
                None, # c2 VARCHAR
                ],
            ]

            print (sInsert)
            cur.execute (sInsert, aaoValues)

            # obtain the warnings and errors for transmitting the data to the database -- the acquisition phase

            sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            print ("con.commit()")
            con.commit ()

            # obtain the warnings and errors for the apply phase

            sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

            sRequest = "{fn teradata_nativesql}{fn teradata_autocommit_on}"
            print (sRequest)
            cur.execute (sRequest)

            sRequest = "SELECT * FROM " + sTableName + " ORDER BY 1"
            print (sRequest)
            cur.execute (sRequest)
            [ print (row) for row in cur.fetchall () ]

        finally:
            sRequest = "DROP TABLE " + sTableName
            print (sRequest)
            cur.execute (sRequest)
