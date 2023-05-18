# Copyright 2020 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to insert and retrieve XML values.
# Use the escape function teradata_parameter to override the data type for a parameter marker bind value.

import json
import teradatasql

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:
    with con.cursor () as cur:
        cur.execute ("create volatile table voltab (c1 integer, c2 xml) on commit preserve rows")

        cur.execute ("{fn teradata_parameter(2,XML)}insert into voltab values (?, ?)", [
            [ 1, "<hello>world</hello>" ],
            [ 2, "<hello>moon</hello>" ],
        ])

        cur.execute ("{fn teradata_fake_result_sets}select * from voltab order by 1")

        # obtain column metadata from the fake result set
        asTypeNames = [ m ["TypeName"] for m in json.loads (cur.fetchone () [7]) ]

        cur.nextset () # advance to the real result set

        for nRow, row in enumerate (cur.fetchall (), 1):
            for iColumn, oValue in enumerate (row):
                print ("Row {} Column {} {:<7} {}".format (nRow, cur.description [iColumn][0], asTypeNames [iColumn], oValue))
