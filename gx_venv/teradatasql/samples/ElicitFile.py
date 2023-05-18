# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to use the Elicit File feature of the Teradata SQL Driver for Python
# to upload the C source file to the Teradata Database and create a User-Defined Function (UDF).
# This sample program requires the udfinc.c source file to be located in the current directory.

import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        print ('Create function')
        cur.execute ("create function myudfinc(integer) returns integer language c no sql parameter style sql external name 'CS!udfinc!udfinc.c!F!udfinc'")

        print ('Execute function')
        cur.execute ("select myudfinc(1)")

        print ('Function returned', cur.fetchone () [0])

        print ('Drop function')
        cur.execute ("drop function myudfinc")
