#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Performing Parallel sort of the @InputTable on @SortingColumnName and @OutputTable will store the output.
    cursr = openconnection.cursor()
    #Query to fetch maximum and minimum column values
    sortquery1 = "SELECT MAX({}), MIN({}) FROM {};".format(SortingColumnName, SortingColumnName, InputTable)
    cursr.execute(sortquery1)
    maxcolumnvalue, mincolumnvalue = cursr.fetchone()
    numberofthreads = 5
    rangedelta = float(maxcolumnvalue - mincolumnvalue) / numberofthreads
    listofthreads = [0]*numberofthreads
    num=0
    #creating threads and tables to sort ratings table
    while num<numberofthreads:
        minimumrange = mincolumnvalue + num * rangedelta
        maximumrange = minimumrange + rangedelta
        #sending inputs to @sortingTable for each thread
        listofthreads[num] = threading.Thread(target=sortingTable, args=(
        InputTable, "temp_table_sort", SortingColumnName, minimumrange, maximumrange, num, openconnection))
        listofthreads[num].start()
        num+=1
    #query to drop a table if it exists
    sortquery2="DROP TABLE IF EXISTS {};".format(OutputTable)
    cursr.execute(sortquery2)
    #query to create an output table
    sortquery3 = "CREATE TABLE {} ( LIKE {} INCLUDING ALL );".format(OutputTable, InputTable)
    cursr.execute(sortquery3)
    num=0
    #query to insert values into output table
    while num< numberofthreads:
        listofthreads[num].join()
        sortquery4 = "INSERT INTO {} SELECT * FROM {};".format(OutputTable, "temp_table_sort" + str(num))
        cursr.execute(sortquery4)
        num+=1
    cursr.close()
    openconnection.commit()

def sortingTable(InputTable, tablename, SortingColumnName, minimumrange, maximumrange, num, openconnection):
    cursr = openconnection.cursor()
    # query to drop a table if it exists
    query1="DROP TABLE IF EXISTS {};".format(tablename + str(num))
    cursr.execute(query1)
    #creating table for each thread
    query2 = "CREATE TABLE {} ( LIKE {} INCLUDING ALL);".format(tablename + str(num), InputTable)
    cursr.execute(query2)
    #inserting values into each thread-table
    if num == 0:
        query3 = "INSERT INTO {} SELECT * FROM {} WHERE {} >= {} AND {} <= {} ORDER BY {} ASC;".format(
            tablename + str(num), InputTable, SortingColumnName, str(minimumrange), SortingColumnName,
            str(maximumrange), SortingColumnName)
    else:
        query3 = "INSERT INTO {} SELECT * FROM {} WHERE {} > {} AND {} <= {} ORDER BY {} ASC;".format(
            tablename + str(num), InputTable, SortingColumnName, str(minimumrange), SortingColumnName,
            str(maximumrange), SortingColumnName)
    cursr.execute(query3)

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    # Performing ParallelJoin on @InputTable1, @InputTable2, @Table1JoinColumn, @Table2JoinColumn and store @OutputTable will store output.
    cursr = openconnection.cursor()
    # Query to fetch maximum and minimum column values from @InputTable1
    joinquery1 = "SELECT MAX({}), MIN({}) FROM {};".format(Table1JoinColumn, Table1JoinColumn, InputTable1)
    cursr.execute(joinquery1)
    maximumcolvalue1, minimumcolvalue1 = cursr.fetchone()
    # Query to fetch maximum and minimum column values from @InputTable2
    joinquery2 = "SELECT MAX({}), MIN({}) FROM {};".format(Table2JoinColumn, Table2JoinColumn, InputTable2)
    cursr.execute(joinquery2)
    maximumcolvalue2, minimumcolvalue2 = cursr.fetchone()
    numberofthreads = 5
    maximumcolvalue = max(maximumcolvalue1, maximumcolvalue2)
    minimumcolvalue = min(minimumcolvalue1, minimumcolvalue2)
    rangedelta = float(maximumcolvalue - minimumcolvalue) / numberofthreads
    #Retrieving data from @InputTable1
    joinquery3 = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}';".format(InputTable1)
    cursr.execute(joinquery3)
    schematable1 = cursr.fetchall()
    # Retrieving data from @InputTable1
    joinquery4 = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}';".format(InputTable2)
    cursr.execute(joinquery4)
    schematable2 = cursr.fetchall()
    listofthreads = [0]*numberofthreads
    num=0
    # creating threads and tables to join ratings and movies tables
    while num<numberofthreads:
        minimumrange = minimumcolvalue + num * rangedelta
        maximumrange = minimumrange + rangedelta
        # sending inputs to @joiningTable for each thread
        listofthreads[num] = threading.Thread(target=joiningTable, args=(
        InputTable1, InputTable2, schematable1, schematable2, "temp_table1_join", "temp_table2_join", Table1JoinColumn,
        Table2JoinColumn, "temp_output_table_join", minimumrange, maximumrange, num, openconnection))
        listofthreads[num].start()
        num+=1
    # query to drop a table if it exists
    joinquery5="DROP TABLE IF EXISTS {};".format(OutputTable)
    cursr.execute(joinquery5)
    # query to create an output table
    joinquery6 = "CREATE TABLE {} ( LIKE {} INCLUDING ALL);".format(OutputTable, InputTable1)
    cursr.execute(joinquery6)
    #Altering output table and adding columns
    joinquery7 = "ALTER TABLE {} ".format(OutputTable)
    num=0
    while num< len(schematable2):
        if num != len(schematable2) - 1:
            joinquery7 += "ADD COLUMN {} {},".format(schematable2[num][0], schematable2[num][1])
        else:
            joinquery7 += "ADD COLUMN {} {};".format(schematable2[num][0], schematable2[num][1])
        num+=1
    cursr.execute(joinquery7)
    num=0
    while num< numberofthreads:
        listofthreads[num].join()
        #inserting values into output table
        joinquery8 = "INSERT INTO {} SELECT * FROM {};".format(OutputTable, "temp_output_table_join" + str(num))
        cursr.execute(joinquery8)
        num+=1
    cursr.close()
    openconnection.commit()

def joiningTable(InputTable1, InputTable2, schematable1, schematable2, table1name, table2name, table1JoinCol, table2JoinCol, outTableName, minimumrange, maximumrange, a, openconnection):
    cursr = openconnection.cursor()
    # query to drop table1 if it exists
    joinquery1="DROP TABLE IF EXISTS {};".format(table1name + str(a))
    cursr.execute(joinquery1)
    # creating table for @InputTable1
    joinquery2="CREATE TABLE {} ( LIKE {} INCLUDING ALL);".format(table1name + str(a), InputTable1)
    cursr.execute(joinquery2)
    # query to drop table2 if it exists
    joinquery3="DROP TABLE IF EXISTS {};".format(table2name + str(a))
    cursr.execute(joinquery3)
    # creating table for @InputTable1
    joinquery4="CREATE TABLE {} ( LIKE {} INCLUDING ALL);".format(table2name + str(a), InputTable2)
    cursr.execute(joinquery4)
    # query to drop output table if it exists
    joinquery5="DROP TABLE IF EXISTS {};".format(outTableName + str(a))
    cursr.execute(joinquery5)
    # creating table for @outTable
    joinquery6="CREATE TABLE {} ( LIKE {} INCLUDING ALL);".format(outTableName + str(a), InputTable1)
    cursr.execute(joinquery6)
    #altering tables and adding columns to output table
    joinquery7= "ALTER TABLE {} ".format(outTableName + str(a))
    num=0
    while num<len(schematable2):
        if num != len(schematable2)-1:
            joinquery7+="ADD COLUMN {} {},".format(schematable2[num][0], schematable2[num][1])
        else:
            joinquery7+="ADD COLUMN {} {};".format(schematable2[num][0], schematable2[num][1])
        num+=1
    cursr.execute(joinquery7)
    #inserting values into table1, table2
    if a==0:
        joinquery8="INSERT INTO {} SELECT * FROM {} WHERE {} >= {} AND {} <= {};".format(table1name + str(a), InputTable1, table1JoinCol, str(minimumrange), table1JoinCol, str(maximumrange))
        cursr.execute(joinquery8)
        joinquery9="INSERT INTO {} SELECT * FROM {} WHERE {} >= {} AND {} <= {};".format(table2name + str(a), InputTable2, table2JoinCol, str(minimumrange), table2JoinCol, str(maximumrange))
        cursr.execute(joinquery9)
    else:
        joinquery10="INSERT INTO {} SELECT * FROM {} WHERE {} > {} AND {} <= {};".format(table1name + str(a), InputTable1, table1JoinCol, str(minimumrange), table1JoinCol, str(maximumrange))
        cursr.execute(joinquery10)
        joinquery11="INSERT INTO {} SELECT * FROM {} WHERE {} > {} AND {} <= {};".format(table2name + str(a), InputTable2, table2JoinCol, str(minimumrange), table2JoinCol, str(maximumrange))
        cursr.execute(joinquery11)
    #inserting values into output table
    joinquery12="INSERT INTO {} SELECT * FROM {} INNER JOIN {} ON {}.{} = {}.{};".format(outTableName + str(a), table1name + str(a), table2name + str(a), table1name + str(a), table1JoinCol, table2name + str(a), table2JoinCol)
    cursr.execute(joinquery12)




################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


