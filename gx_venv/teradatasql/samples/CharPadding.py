# Copyright 2019 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to work with the Teradata Database's Character Export Width behavior.

# The Teradata SQL Driver for Python always uses the UTF8 session character set, and the charset connection
# parameter is not supported. Be aware of the Teradata Database's Character Export Width behavior that adds trailing
# space padding to fixed-width CHAR data type result set column values when using the UTF8 session character set.

# Work around this drawback by using CAST or TRIM in SQL SELECT statements, or in views, to convert fixed-width CHAR
# data types to VARCHAR.

import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        cur.execute ("CREATE TABLE MyTable (c1 CHAR(10), c2 CHAR(10))")
        try:
            cur.execute ("INSERT INTO MyTable VALUES ('a', 'b')")

            print ("Original query that produces trailing space padding:")
            cur.execute ("SELECT c1, c2 FROM MyTable")
            [ print (row) for row in cur.fetchall () ]

            print ("Modified query with either CAST or TRIM to avoid trailing space padding:")
            cur.execute ("SELECT CAST(c1 AS VARCHAR(10)), TRIM(TRAILING FROM c2) FROM MyTable")
            [ print (row) for row in cur.fetchall () ]

            cur.execute ("CREATE VIEW MyView (c1, c2) AS SELECT CAST(c1 AS VARCHAR(10)), TRIM(TRAILING FROM c2) FROM MyTable")
            try:
                print ("Or query view with CAST or TRIM to avoid trailing space padding:")
                cur.execute ("SELECT c1, c2 FROM MyView")
                [ print (row) for row in cur.fetchall () ]
            finally:
                cur.execute ("DROP VIEW MyView")
        finally:
            cur.execute ("DROP TABLE MyTable")
