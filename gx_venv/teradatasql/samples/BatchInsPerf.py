# Copyright 2019 by Teradata Corporation. All rights reserved.

# This sample program measures the time to insert one million rows as 100 batches of 10000 rows per batch.

import sys
import teradatasql
import time

sTableName = "batchinsperf"
nRowsPerBatch = 10000
nBatchCount = 100

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:

    with con.cursor () as cur:

        try:
            cur.execute ("drop table " + sTableName)
        except Exception as ex:
            print ("Ignoring", str (ex).split ("\n") [0])

        cur.execute ("create table " + sTableName + " (c1 integer, c2 varchar(100), c3 integer, c4 varchar(100))")
        try:
            dStartTime1 = time.time ()

            for iBatch in range (0, nBatchCount):

                aaoBatch = []
                for nRow in range (1, nRowsPerBatch + 1):
                    nValue = iBatch * nRowsPerBatch + nRow
                    aaoBatch += [[nValue, "a" + str (nValue), -nValue, "b" + str (nValue)]]

                dStartTime2 = time.time ()

                cur.execute ("insert into " + sTableName + " values (?, ?, ?, ?)", aaoBatch)

                print ("Batch insert #{} of {} rows took {:.0f} ms".format (iBatch + 1, nRowsPerBatch, (time.time () - dStartTime2) * 1000))

                # end for iBatch

            dTotalTime = time.time () - dStartTime1
            print ("Inserting {} batches containing {} total rows took {:.2f} sec for {:.2f} rows/sec throughput".format (
                nBatchCount,
                nBatchCount * nRowsPerBatch,
                dTotalTime,
                nBatchCount * nRowsPerBatch / dTotalTime
            ))

            cur.execute ("select count(*) from " + sTableName + " order by 1")
            print ("Table {} contains {} rows".format (sTableName, cur.fetchone () [0]))

        finally:
            cur.execute ("drop table " + sTableName)
