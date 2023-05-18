# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to insert a batch of rows.

import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        cur.execute ("create volatile table voltab (c1 integer, c2 varchar(100)) on commit preserve rows")

        cur.execute ("insert into voltab (?, ?)", [
            [1, "abc"],
            [2, "def"],
            [3, "ghi"]])

        cur.execute ("select * from voltab order by 1")
        [ print (row) for row in cur.fetchall () ]
