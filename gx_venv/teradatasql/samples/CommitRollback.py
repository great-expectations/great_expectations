# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to use the commit and rollback methods after turning off auto-commit.

import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        cur.execute ("{fn teradata_nativesql}{fn teradata_autocommit_off}")

        cur.execute ("create volatile table voltab (c1 integer) on commit preserve rows")
        con.commit ()

        cur.execute ("insert into voltab values (1)")
        con.commit ()

        cur.execute ("insert into voltab values (2)")
        con.rollback ()

        cur.execute ("insert into voltab values (3)")
        cur.execute ("insert into voltab values (4)")
        con.commit ()

        cur.execute ("insert into voltab values (5)")
        cur.execute ("insert into voltab values (6)")
        con.rollback ()

        cur.execute ("select * from voltab order by 1")
        con.commit ()

        anValues = [ row [0] for row in cur.fetchall () ]

        print ("Expected result set row values: [1, 3, 4]")
        print ("Obtained result set row values: {}".format (anValues))
