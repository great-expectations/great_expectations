# Copyright 2018 by Teradata Corporation. All rights reserved.

# This sample program demonstrates how to create and call a SQL stored procedure
# with a variety of parameters and dynamic result sets.

import teradatasql

def DisplayResults (cur):
    while True:
        print (" === metadata ===")
        print ("  cur.rowcount:", cur.rowcount)
        print ("  cur.description:", cur.description)

        print (" === result ===")
        [ print (" ", row) for row in cur.fetchall () ]

        if not cur.nextset ():
            break

def cur_execute (cur, sSQL, params=None):
    print ()
    print ("cur.execute", sSQL, "bound values", params)
    cur.execute (sSQL, params)
    DisplayResults (cur)

def cur_callproc (cur, sProcName, params=None):
    print ()
    print ("cur.callproc", sProcName, "bound values", params)
    cur.callproc (sProcName, params) # OUT parameters are not supported by .callproc
    DisplayResults (cur)

with teradatasql.connect ('{"host":"whomooz","user":"guest","password":"please"}') as con:
    with con.cursor () as cur:

        # Demonstrate a stored procedure having IN and INOUT parameters.
        # Returns one result set having one row and one column containing the output parameter value.

        cur.execute ("replace procedure examplestoredproc (in p1 integer, inout p2 integer) begin set p2 = p1 + p2 ; end ;")
        try:
            cur_execute (cur, "{call examplestoredproc (3, 5)}") # literal parameter values
            cur_execute (cur, "{call examplestoredproc (?, ?)}", [10, 7]) # bound parameter values
            cur_callproc (cur, "examplestoredproc", [20, 4]) # bound parameter values

            # Demonstrate a stored procedure having one OUT parameter.
            # Returns one result set having one row and one column containing the output parameter value.
            # Only demonstrate .execute because OUT parameters are not supported by .callproc
            # OUT parameters must be unbound.

            cur.execute ("replace procedure examplestoredproc (out p1 varchar(100)) begin set p1 = 'foobar' ; end ;")
            cur_execute (cur, "{call examplestoredproc (?)}")

            # Demonstrate a stored procedure having no parameters that returns one dynamic result set.
            # Returns two result sets.
            # The first result set is empty having no rows or columns, because there are no output parameter values.
            # The second result set is the dynamic result set returned by the stored procedure.

            cur.execute ("""replace procedure examplestoredproc ()
                dynamic result sets 1
                begin
                    declare cur1 cursor with return for select * from dbc.dbcinfo order by 1 ;
                    open cur1 ;
                end ;""")
            cur_execute (cur, "{call examplestoredproc}")
            cur_callproc (cur, "examplestoredproc")

            # Demonstrate a stored procedure having IN and INOUT parameters that returns two dynamic result sets.
            # Returns three result sets.
            # The first result set has one row and one column containing the output parameter values.
            # The second and third result sets are dynamic result sets returned by the stored procedure.

            cur.execute ("""replace procedure examplestoredproc (in p1 integer, inout p2 integer, inout p3 integer)
                dynamic result sets 2
                begin
                    declare cur1 cursor with return for select * from dbc.dbcinfo order by 1 ;
                    declare cur2 cursor with return for select infodata, infokey from dbc.dbcinfo order by 1 ;
                    open cur1 ;
                    open cur2 ;
                    set p2 = p1 + p2 ;
                    set p3 = p1 * p3 ;
                end ;""")
            cur_execute (cur, "{call examplestoredproc (2, 1, 3)}") # literal parameter values
            cur_execute (cur, "{call examplestoredproc (?, ?, ?)}", [3, 2, 4]) # bound IN and INOUT parameter values
            cur_callproc (cur, "examplestoredproc", [10, 3, 2]) # bound IN and INOUT parameter values

            # Demonstrate a stored procedure having IN, INOUT, and OUT parameters that returns two dynamic result sets.
            # Returns three result sets.
            # The first result set has one row and two columns containing the output values from the INOUT and OUT parameters.
            # The second and third result sets are dynamic result sets returned by the stored procedure.
            # Only demonstrate .execute because OUT parameters are not supported by .callproc
            # OUT parameters must be unbound.

            cur.execute ("""replace procedure examplestoredproc (in p1 integer, inout p2 integer, out p3 varchar(100))
                dynamic result sets 2
                begin
                    declare cur1 cursor with return for select * from dbc.dbcinfo order by 1 desc ;
                    declare cur2 cursor with return for select infodata, infokey from dbc.dbcinfo order by 1 ;
                    open cur1 ;
                    open cur2 ;
                    set p2 = p1 + p2 ;
                    set p3 = 'hello' ;
                end ;""")
            cur_execute (cur, "{call examplestoredproc (10, 5, ?)}") # literal parameter values
            cur_execute (cur, "{call examplestoredproc (?, ?, ?)}", [20, 7]) # bound IN and INOUT parameter values

        finally:
            cur.execute ("drop procedure examplestoredproc")
