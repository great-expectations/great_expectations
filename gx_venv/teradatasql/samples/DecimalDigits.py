# Copyright 2022 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to format decimal.Decimal values.

import decimal
import teradatasql

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:
    with con.cursor () as cur:
        cur.execute ("create volatile table voltab (c1 integer, c2 decimal(15,8), c3 varchar(10)) on commit preserve rows")
        cur.execute ("insert into voltab values (?, ?, ?)", [
            [123, decimal.Decimal ("0.00000081"), "hello"],
            [456, None, "world"],
            [789, decimal.Decimal ("1234567.89012345"), None],
        ])

        print ("Without decimal.Decimal formatting, Row 1 Column 2 value is printed as 8.1E-7")
        cur.execute ("select * from voltab order by 1")
        for iRow, row in enumerate (cur.fetchall ()):
            for iColumn, value in enumerate (row):
                print ("Row {} Column {} value = {}".format (iRow + 1, iColumn + 1, value))

        print ("With decimal.Decimal formatting, Row 1 Column 2 value is printed as 0.00000081")
        cur.execute ("select * from voltab order by 1")
        for iRow, row in enumerate (cur.fetchall ()):
            for iColumn, value in enumerate (row):
                if isinstance (value, decimal.Decimal):
                    value = "{:.{scale}f}".format (value, scale=cur.description [iColumn][5])
                print ("Row {} Column {} value = {}".format (iRow + 1, iColumn + 1, value))
