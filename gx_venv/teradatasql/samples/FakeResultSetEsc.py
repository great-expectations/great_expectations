# Copyright 2019 by Teradata Corporation. All rights reserved.

# This sample program demonstrates the behavior of the fake_result_sets escape function.

import teradatasql

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:
    with con.cursor () as cur:
        cur.execute ("create volatile table voltab1 (c1 integer, c2 varchar(100)) on commit preserve rows")
        cur.execute ("insert into voltab1 values (1, 'abc')")
        cur.execute ("insert into voltab1 values (2, 'def')")

        cur.execute ("create volatile table voltab2 (c1 integer, c2 varchar(100)) on commit preserve rows")
        cur.execute ("insert into voltab2 values (3, 'ghi')")
        cur.execute ("insert into voltab2 values (4, 'jkl')")
        cur.execute ("insert into voltab2 values (5, 'mno')")

        cur.execute ("create volatile table voltab3 (c1 integer, c2 varchar(100)) on commit preserve rows")
        cur.execute ("insert into voltab3 values (6, 'pqr')")
        cur.execute ("insert into voltab3 values (7, 'stu')")
        cur.execute ("insert into voltab3 values (8, 'vwx')")
        cur.execute ("insert into voltab3 values (9, 'yz')")

        cur.execute ("{fn teradata_fake_result_sets}select * from voltab1 order by 1")
        print ("=== Two result sets produced by a single-statement SQL request that returns one result set ===")
        print (" --- Fake result set ---")
        row = cur.fetchone ()
        [ print (" Column {} {:15} = {}".format (i + 1, cur.description [i][0], row [i])) for i in range (0, len (row)) ]
        cur.nextset ()
        print (" --- Real result set ---")
        [ print (row) for row in cur.fetchall () ]
        print ()

        cur.execute ("{fn teradata_fake_result_sets}select * from voltab2 order by 1 ; select * from voltab3 order by 1")
        print ("=== Four result sets produced by a multi-statement SQL request that returns two result sets ===")
        print (" --- Fake result set ---")
        row = cur.fetchone ()
        [ print (" Column {} {:15} = {}".format (i + 1, cur.description [i][0], row [i])) for i in range (0, len (row)) ]
        cur.nextset ()
        print (" --- Real result set ---")
        [ print (row) for row in cur.fetchall () ]
        cur.nextset ()
        print (" --- Fake result set ---")
        row = cur.fetchone ()
        [ print (" Column {} {:15} = {}".format (i + 1, cur.description [i][0], row [i])) for i in range (0, len (row)) ]
        cur.nextset ()
        print (" --- Real result set ---")
        [ print (row) for row in cur.fetchall () ]
