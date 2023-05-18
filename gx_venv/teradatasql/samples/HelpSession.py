# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to obtain and display session information.

import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        cur.execute ('help session')
        row = cur.fetchone ()
        for i in range (0, len (row)):
            print ('{:>40}  {}'.format (cur.description [i][0], row [i]))
