# Copyright 2021 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to insert a batch of rows using a CSV file.

import csv
import os
import teradatasql

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:
    with con.cursor () as cur:
        cur.execute ("create volatile table voltab (c1 integer, c2 varchar(100)) on commit preserve rows")

        records = [
            ["c1", "c2"],
            ["1", ""],
            ["2", "abc"],
            ["3", "def"],
            ["4", "mno"],
            ["5", ""],
            ["6", "pqr"],
            ["7", "uvw"],
            ["8", "xyz"],
            ["9", ""],
        ]

        print ("Create csv file: %s" % records)

        sFileName = "CSVBatchInsertData_py"
        with open (sFileName, 'w', encoding='UTF8') as f:
            writer = csv.writer (f)
            for iIndex in range (len (records)):
                writer.writerow (records [iIndex])

        try:
            print ("Inserting data")
            cur.execute ("{fn teradata_read_csv(%s)} insert into voltab (?, ?)" % sFileName)
        finally:
            os.remove (sFileName)

        cur.execute ("select * from voltab order by 1")
        [ print (row) for row in cur.fetchall () ]
