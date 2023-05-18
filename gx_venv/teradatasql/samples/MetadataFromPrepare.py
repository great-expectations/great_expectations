# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to use the teradata_rpo(S) and teradata_fake_result_sets
# escape functions to prepare a SQL request without executing it and obtain SQL statement metadata.
# This sample program assumes that StatementInfo parcel support is available from the Teradata Database.

import json
import teradatasql

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:
        cur.execute ('{fn teradata_rpo(S)}{fn teradata_fake_result_sets}select * from dbc.dbcinfo where infokey=?')
        row = cur.fetchone ()
        print ('SQL statement metadata from prepare operation:')
        print ()
        for i in range (0, len (cur.description)):
            print ('   Column [{}] {:>18} : {}'.format (i, cur.description [i][0], row [i]))
        print ()
        print ('Result set column metadata as pretty-printed JSON:')
        print (json.dumps (json.loads (row [7]), indent=4, sort_keys=True))
        print ()
        print ('Parameter marker metadata as pretty-printed JSON:')
        print (json.dumps (json.loads (row [8]), indent=4, sort_keys=True))
