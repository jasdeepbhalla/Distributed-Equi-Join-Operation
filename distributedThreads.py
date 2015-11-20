#!/usr/bin/python2.7
#
# Assignment3 Interface ~ Jasdeep
#

import threading
import psycopg2
import os
import sys
import math

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column1'
RangePartPrefix = 'rangeratingspart'
ParallelPart = 'parallelPart'
JoinPartPrefix = 'joinpart'

##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    #loading intitial table
    #loadTables(FIRST_TABLE_NAME, "test_data.dat", con);

    cursor = openconnection.cursor()
    cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s';"%InputTable)
    rows = cursor.fetchall()

    columnValues = ''
    for row in rows:
        columnValues = columnValues + row[0] +' '+ row[1] + ', '
    columnNames = columnValues[:-2]

    #create outputTable
    cursor.execute("CREATE TABLE IF NOT EXISTS "+OutputTable +"("+columnNames+");")

    cursor.execute('SELECT MIN(%s) FROM %s;'%(SortingColumnName,InputTable))
    minValue = cursor.fetchone()[0]

    cursor.execute('SELECT MAX(%s) FROM %s;'%(SortingColumnName,InputTable))
    maxValue = cursor.fetchone()[0]

    step = int(math.ceil((maxValue - minValue)/5.0))

    min = minValue
    max = min + step

    for i in range(0,5):
        cursor.execute('CREATE TABLE IF NOT EXISTS '+ ParallelPart+ str(i) +'('+columnNames+');')
        if i<4:
            cursor.execute("INSERT INTO "+ ParallelPart+ str(i) + " SELECT * from "+InputTable+" WHERE "+SortingColumnName+" >= "+str(min)+" AND "+SortingColumnName+" < "+str(max))
        else:
            cursor.execute("INSERT INTO "+ ParallelPart+ str(i) + " SELECT * from "+InputTable+" WHERE "+SortingColumnName+" >= "+str(min)+" AND "+SortingColumnName+" <= "+str(max))
        min=max
        max=min + step

    # 5 threads ----------------------------------------------------------------------
    t0 = threading.Thread(target=ParallelSorting, args=(ParallelPart+str(0),SortingColumnName,OutputTable,openconnection))
    t0.start()

    t1 = threading.Thread(target=ParallelSorting, args=(ParallelPart+str(1),SortingColumnName,OutputTable,openconnection))
    t1.start()

    t2 = threading.Thread(target=ParallelSorting, args=(ParallelPart+str(2),SortingColumnName,OutputTable,openconnection))
    t2.start()

    t3 = threading.Thread(target=ParallelSorting, args=(ParallelPart+str(3),SortingColumnName,OutputTable,openconnection))
    t3.start()

    t4 = threading.Thread(target=ParallelSorting, args=(ParallelPart+str(4),SortingColumnName,OutputTable,openconnection))
    t4.start()

    t0.join()
    t1.join()
    t2.join()
    t3.join()
    t4.join()

    deletePartitions(openconnection)
    openconnection.commit()

def ParallelSorting(rangePart, SortingColumnName,OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("INSERT INTO %s Select * FROM %s ORDER BY %s ASC " %(OutputTable,rangePart, SortingColumnName));

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    cursor = openconnection.cursor()

    #table1 -----------------------------------------------------------------------------------------------------------
    cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s';"%InputTable1)
    rows1 = cursor.fetchall()

    columnValuesTable1 = ''
    for row in rows1:
        columnValuesTable1 = columnValuesTable1 + row[0] +' '+ row[1] + ', '
    columnNamesTable1 = columnValuesTable1[:-2]

    #table2 -----------------------------------------------------------------------------------------------------------
    cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s';"%InputTable2)
    rows2 = cursor.fetchall()

    columnValuesTable2 = ''
    for row in rows2:
        if row[0] == Table1JoinColumn:
            columnValuesTable2 = columnValuesTable2 + row[0]+'_new '+ row[1] + ', '
        else:
            columnValuesTable2 = columnValuesTable2 + row[0] +' '+ row[1] + ', '
    columnNamesTable2 = columnValuesTable2[:-2]

    #new columns to make schema for third table -----------------------------------------------------------------------
    combinedNewColumns = columnNamesTable1 + ', '+ columnNamesTable2

    #create outputTable -----------------------------------------------------------------------------------------------
    cursor.execute("CREATE TABLE IF NOT EXISTS "+OutputTable +"("+combinedNewColumns+");")

    cursor.execute("SELECT COUNT(*) FROM " + InputTable1)
    totalTable1Rows = cursor.fetchone()[0]

    step = int(math.ceil(totalTable1Rows/5.0))
    limit = step

    for i in range(0,5):
        offset = step * i
        cursor.execute('CREATE TABLE IF NOT EXISTS '+ JoinPartPrefix+ str(i) +'('+columnNamesTable1+');')
        cursor.execute("INSERT INTO "+ JoinPartPrefix+ str(i) + " SELECT * from "+InputTable1+" LIMIT "+str(limit)+" OFFSET "+str(offset)+";")

    # parallel processing ---------------------------------------------------------------------------------------------
    t0 = threading.Thread(target=ParallelJoining, args=(JoinPartPrefix+str(0), InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t0.start()

    t1 = threading.Thread(target=ParallelJoining, args=(JoinPartPrefix+str(1), InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t1.start()

    t2 = threading.Thread(target=ParallelJoining, args=(JoinPartPrefix+str(2), InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t2.start()

    t3 = threading.Thread(target=ParallelJoining, args=(JoinPartPrefix+str(3), InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t3.start()

    t4 = threading.Thread(target=ParallelJoining, args=(JoinPartPrefix+str(4), InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t4.start()

    t0.join()
    t1.join()
    t2.join()
    t3.join()
    t4.join()

    deletePartitions(openconnection)
    openconnection.commit()

def ParallelJoining (joinPart, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("INSERT INTO "+OutputTable+" SELECT * FROM "+joinPart+" INNER JOIN "+InputTable2+" ON "+str(joinPart)+"."+str(Table1JoinColumn)+"="+str(InputTable2)+"."+str(Table2JoinColumn));

def deletePartitions(openconnection):
    cur = openconnection.cursor()

    for i in range(0,5):
        cur.execute("DROP TABLE IF EXISTS %s"%(ParallelPart+str(i)))

    for i in range(0,5):
        cur.execute("DROP TABLE IF EXISTS %s"%(JoinPartPrefix+str(i)))

    cur.close();
    openconnection.commit()

#loading table (not used)
def loadTables(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS "+ratingstablename+";")
    cur.execute("CREATE TABLE IF NOT EXISTS "+ratingstablename+" (MovieID INT, column1 REAL, Collection INT)")
    loadout = open(ratingsfilepath,'r')
    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('MovieID','column1','Collection'))

    cur.execute("DROP TABLE IF EXISTS "+SECOND_TABLE_NAME+";")
    cur.execute("CREATE TABLE IF NOT EXISTS "+SECOND_TABLE_NAME+" (MovieIDs INT, column1 REAL, Collections INT)")
    loadout = open("test_data2.dat",'r')
    cur.copy_from(loadout,SECOND_TABLE_NAME,sep = ':',columns=('MovieIDs','column1','Collections'))

    cur.close()
    openconnection.commit()


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
        print 'A database named {0} already exists'.format(dbname)

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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
