# Copyright 2021 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to display the output from a show command.
# The show command output includes embedded carriage returns as line terminators,
# which typically must be changed to newlines in order to display properly.
# The show command output may include multiple result sets, and the cursor nextset
# function must be called to advance to subsequent result sets.

import teradatasql

def ShowCommand (cur, s):
    print ("-- " + s)
    cur.execute (s)
    n = 1
    while True:
        print ("-- result set {}".format (n))
        rows = cur.fetchall ()
        for row in rows:
            print ("".join ([ str (col).replace ("\r", "\n") for col in row ]))
        if cur.nextset ():
            n += 1
        else:
            break

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:
    with con.cursor () as cur:
        ShowCommand (cur, "show view DBC.DBCInfo")
        print ()
        ShowCommand (cur, "show select * from DBC.DBCInfo")
