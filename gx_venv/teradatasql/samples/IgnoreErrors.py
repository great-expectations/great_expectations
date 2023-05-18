# Copyright 2019 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to ignore errors.

import teradatasql

with teradatasql.connect (host="whomooz", user="guest", password="please") as con:

    with con.cursor () as cur:

        print ("Demonstrating how to ignore caught exceptions:")

        print ("drop user NonExistentUser")
        try:
            cur.execute ("drop user NonExistentUser")
        except Exception as ex:
            if "[Error 3802]" in str (ex):
                print ("Ignoring", str (ex).split ("\n") [0])
            else:
                raise # rethrow

        print ("drop view NonExistentView")
        try:
            cur.execute ("drop view NonExistentView")
        except Exception as ex:
            if "[Error 3807]" in str (ex):
                print ("Ignoring", str (ex).split ("\n") [0])
            else:
                raise # rethrow

        print ("drop macro NonExistentMacro")
        try:
            cur.execute ("drop macro NonExistentMacro")
        except Exception as ex:
            if "[Error 3824]" in str (ex):
                print ("Ignoring", str (ex).split ("\n") [0])
            else:
                raise # rethrow

        print ("drop table NonExistentTable")
        try:
            cur.execute ("drop table NonExistentTable")
        except Exception as ex:
            if "[Error 3807]" in str (ex):
                print ("Ignoring", str (ex).split ("\n") [0])
            else:
                raise # rethrow

        print ("drop database NonExistentDbase")
        try:
            cur.execute ("drop database NonExistentDbase")
        except Exception as ex:
            if "[Error 3802]" in str (ex):
                print ("Ignoring", str (ex).split ("\n") [0])
            else:
                raise # rethrow

        print ("drop procedure NonExistentProc")
        try:
            cur.execute ("drop procedure NonExistentProc")
        except Exception as ex:
            if "[Error 5495]" in str (ex):
                print ("Ignoring", str (ex).split ("\n") [0])
            else:
                raise # rethrow

        print ()
        print ("Demonstrating how to ignore a single error:")
        print ("drop table NonExistentTable")
        cur.execute ("drop table NonExistentTable", ignoreErrors=3807)

        print ()
        print ("Demonstrating how to ignore several different errors:")

        nonExistenceErrors = (
            3526, # The specified index does not exist.
            3802, # Database '%VSTR' does not exist.
            3807, # Object '%VSTR' does not exist.
            3824, # Macro '%VSTR' does not exist.
            3913, # The specified check does not exist.
            4322, # Schema %VSTR does not exist # DR176193
            5322, # The specified constraint name '%VSTR' does not exist.
            5495, # Stored Procedure "%VSTR" does not exist.
            5589, # Function "%VSTR" does not exist.
            5620, # Role '%VSTR' does not exist.
            5623, # User or role '%VSTR' does not exist.
            5653, # Profile '%VSTR' does not exist.
            5901, # Replication Group '%VSTR' does not exist.
            6808, # Ordering is not defined for UDT '%TVMID'.
            6831, # UDT "%VSTR" does not exist.
            6834, # Method "%VSTR" does not exist.
            6849, # The UDT (%VSTR) does not have Transform, or does not have the specified Transform Group.
            6863, # Cast with specified source and target does not exist
            6934, # External Stored Procedure "%VSTR" does not exist.
            6938, # Authorization "%VSTR" does not exist.
            7972, # JAVA Stored Procedure "%VSTR" does not exist.
            9213, # Connect Through privilege for %VSTR not found
            9403, # Specified constraint name "%VSTR" does not exist
            )

        print ("drop user NonExistentUser")
        cur.execute ("drop user NonExistentUser", ignoreErrors=nonExistenceErrors)
        print ("drop view NonExistentView")
        cur.execute ("drop view NonExistentView", ignoreErrors=nonExistenceErrors)
        print ("drop macro NonExistentMacro")
        cur.execute ("drop macro NonExistentMacro", ignoreErrors=nonExistenceErrors)
        print ("drop table NonExistentTable")
        cur.execute ("drop table NonExistentTable", ignoreErrors=nonExistenceErrors)
        print ("drop database NonExistentDbase")
        cur.execute ("drop database NonExistentDbase", ignoreErrors=nonExistenceErrors)
        print ("drop procedure NonExistentProc")
        cur.execute ("drop procedure NonExistentProc", ignoreErrors=nonExistenceErrors)
