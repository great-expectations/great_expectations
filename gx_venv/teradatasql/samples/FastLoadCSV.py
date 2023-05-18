# Copyright 2020 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to FastLoad a CSV file.

import csv
import os
import teradatasql

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:
    with con.cursor () as cur:
        sTableName = "FastLoadCSV"
        try:
            sRequest = "DROP TABLE " + sTableName
            print (sRequest)
            cur.execute (sRequest)
        except Exception as ex:
            print ("Ignoring", str (ex).split ("\n") [0])

        try:
            sRequest = "DROP TABLE " + sTableName + "_ERR_1"
            print (sRequest)
            cur.execute (sRequest)
        except Exception as ex:
            print ("Ignoring", str (ex).split ("\n") [0])

        try:
            sRequest = "DROP TABLE " + sTableName + "_ERR_2"
            print (sRequest)
            cur.execute (sRequest)
        except Exception as ex:
            print ("Ignoring", str (ex).split ("\n") [0])

        records = [
            "c1,c2",
            "1,",
            "2,abc",
            "3,def",
            "4,mno",
            "5,",
            "6,pqr",
            "7,uvw",
            "8,xyz",
            "9,",
        ]
        print ("records", records)

        csvFileName = "dataPy.csv"
        print ("open", csvFileName)
        with open (csvFileName, 'w', newline='') as csvFile:
            csvWriter = csv.writer (csvFile, delimiter='\n')
            csvWriter.writerow (records)

        try:
            sRequest = "CREATE TABLE " + sTableName + " (c1 INTEGER NOT NULL, c2 VARCHAR(10))"
            print (sRequest)
            cur.execute (sRequest)

            try:
                sRequest = "{fn teradata_nativesql}{fn teradata_autocommit_off}"
                print (sRequest)
                cur.execute (sRequest)

                sInsert = "{fn teradata_require_fastload}{fn teradata_read_csv(" + csvFileName + ")}INSERT INTO " + sTableName + " (?, ?)"
                print (sInsert)
                cur.execute (sInsert)

                # obtain the warnings and errors for transmitting the data to the database -- the acquisition phase

                sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
                print (sRequest)
                cur.execute (sRequest)
                [ print (row) for row in cur.fetchall () ]

                sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
                print (sRequest)
                cur.execute (sRequest)
                [ print (row) for row in cur.fetchall () ]

                sRequest = "{fn teradata_nativesql}{fn teradata_logon_sequence_number}" + sInsert
                print (sRequest)
                cur.execute (sRequest)
                [ print (row) for row in cur.fetchall () ]

                print ("con.commit()")
                con.commit ()

                # obtain the warnings and errors for the apply phase

                sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
                print (sRequest)
                cur.execute (sRequest)
                [ print (row) for row in cur.fetchall () ]

                sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
                print (sRequest)
                cur.execute (sRequest)
                [ print (row) for row in cur.fetchall () ]

                sRequest = "{fn teradata_nativesql}{fn teradata_autocommit_on}"
                print (sRequest)
                cur.execute (sRequest)

                sRequest = "SELECT * FROM " + sTableName + " ORDER BY 1"
                print (sRequest)
                cur.execute (sRequest)
                [ print (row) for row in cur.fetchall () ]

            finally:
                sRequest = "DROP TABLE " + sTableName
                print (sRequest)
                cur.execute (sRequest)

        finally:
            print ("os.remove", csvFileName)
            os.remove (csvFileName)
