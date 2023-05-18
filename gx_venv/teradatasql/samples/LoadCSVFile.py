# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to use the Python csv module to parse data values
# from a CSV file, and then insert those data values into a database table.
# This sample program requires the airports.csv file to be located in the current directory.

import csv
import teradatasql

with open ('airports.csv', newline='') as f:
    with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
        with con.cursor () as cur:
            cur.execute ("create volatile table Airports (City varchar(100), Airport varchar(100), AirportCode varchar(10)) on commit preserve rows")
            cur.execute ("insert into Airports (?, ?, ?)", [ row for row in csv.reader (f) ])
            cur.execute ("select AirportCode, Airport, City from Airports order by AirportCode")
            [ print (row) for row in cur.fetchall () ]
