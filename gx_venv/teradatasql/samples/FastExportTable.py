# Copyright 2020 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to FastExport rows from a table.

import teradatasql

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:
    with con.cursor () as cur:
        with con.cursor () as cur2:
            sTableName = "FastExportTable"
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
                sInsert = "INSERT INTO " + sTableName + " VALUES (?, ?)"
                print (sInsert)
                cur.execute (sInsert, [
                    [1, None],
                    [2, "abc"],
                    [3, "def"],
                    [4, "mno"],
                    [5, None],
                    [6, "pqr"],
                    [7, "uvw"],
                    [8, "xyz"],
                    [9, None],
                ])

                sSelect = "{fn teradata_try_fastexport}SELECT * FROM " + sTableName
                print (sSelect)
                cur.execute (sSelect)
                [ print (row) for row in sorted (cur.fetchall ()) ]

                sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sSelect
                print (sRequest)
                cur2.execute (sRequest)
                [ print (row) for row in cur2.fetchall () ]

                sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sSelect
                print (sRequest)
                cur2.execute (sRequest)
                [ print (row) for row in cur2.fetchall () ]

                sRequest = "{fn teradata_nativesql}{fn teradata_logon_sequence_number}" + sSelect
                print (sRequest)
                cur2.execute (sRequest)
                [ print (row) for row in cur2.fetchall () ]

            finally:
                sRequest = "DROP TABLE " + sTableName
                print (sRequest)
                cur.execute (sRequest)
