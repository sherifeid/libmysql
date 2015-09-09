__author__ = 'Sherif Eid'

"""
This file includes simple database manipulation functions to connect, create and delete databases and tables
"""

import sys
import pymysql

class bcolors:
    """
    Class used to store colors to message printing
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def connect_sql(server, port, username, password, database):
    """
    Connects to the database with the given input credentials and returns a database cursor
    for more details refer to PyMSQL examples (https://pypi.python.org/pypi/PyMySQL)

    :param server: MySQL/MariaDB server address
    :type server: str
    :param port: Database communication port
    :param username: Username
    :type username: str
    :param password:  Password
    :type password: str
    :param database: Database to connect to
    :type database: str

    :return: Returns database cursor object
    """
    try:
        db = pymysql.Connect(host=server, port=int(port), user=username, passwd=password, db=database)
        curs = db.cursor()
        print(bcolors.OKGREEN + 'OK: Connected successfully to ' + server + ' through port ' + str(port) + " ..." + bcolors.ENDC)
    except:
        print(bcolors.FAIL + 'ERROR: Unable to connect to ' + server + ' through port ' + str(port))
        print('Check server address, port or username/password combination ...' + bcolors.ENDC)
        sys.exit(0)

    # returns cursor
    return curs

def get_databases(cursor):
    """
    This function lists all the databases in the current server

    :param cursor: database cursor, returned from sqlconnect() function
    :type cursor:object

    :return: Returns all databases as a list of strings
    """
    cursor.execute('SHOW DATABASES;')       # flush, remove table
    tmp = cursor.fetchall()                 # returns a list of tuples (weird)
    dblist = []
    for i in tmp:
        dblist.append(list(i)[0])           # take the first element of the list(tuple)
    return dblist

def get_currentdb(cursor):
    """
    This function gets the currently selected database

    :param cursor: database cursor, returned from sqlconnect() function
    :type cursor:object

    :return: Returns current database
    """
    cursor.execute('SELECT DATABASE();')       # flush, remove table
    tmp = cursor.fetchall()                 # returns a list of tuples (weird)
    dblist = []
    for i in tmp:
        dblist.append(list(i)[0])           # take the first element of the list(tuple)
    return dblist[0]


def get_tables(cursor):
    """
    This function lists all the tables in the currently selected database

    :param cursor: database cursor, returned from sqlconnect() function
    :type cursor:object

    :return: Returns all databases as a list of strings
    """
    cursor.execute('SHOW TABLES;')       # flush, remove table
    tmp = cursor.fetchall()                 # returns a list of tuples
    tablelist = []
    for i in tmp:
        tablelist.append(list(i)[0])           # take the first element of the list(tuple)
    return tablelist

def select_database(cursor, database):
    """
    This function selects a specific database

    :param cursor: database cursor, returned from sqlconnect() function
    :type cursor:object
    :param database: database to select
    :type database: str

    :return:    0 if database selection is OK
                -1 if there's a problem selecting the database
    """
    try:
        cursor.execute('USE ' + database + ';')
        print(bcolors.OKGREEN + 'Using database : ' + database)
        return 0
    except:
        print(bcolors.FAIL + 'Unable to select database : ' + database + bcolors.ENDC)
        return -1

def create_table(cursor, table_name, header,  safe=True):
    """
    This function creates a table in the currently used database with the given header list. If safe is set to True then the function
    will check if the table exists, if it does then it doesn't create it. If safe is set to False then
    the function will delete the table if it exists then recreate it.

    :param cursor: database cursor
    :type cursor: object
    :param table_name: name of table to be created
    :type table_name: str
    :param header: a list describing the table column names and types
    :type header: list
    :param safe: (Optional, defaults to True) safe creation of table
    :type safe: bool

    :return: Returns 0 if created successfully, -1 if an error is encountered
    """

    # compile table command from header list


    tblcols = "("

    for i in header:
        tblcols += i[0] + ' ' + i[1] + ','

    tblcols = tblcols.rstrip(',')       # remove right most ','
    tblcols += ')'

    try:
        if safe == True:
            # safely create the table here
            sqlcmd = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' ' + tblcols + ';'
            cursor.execute(sqlcmd)
        else:
            # delete table if found
            sqlcmd = 'DROP TABLE IF EXISTS ' + table_name + ';'
            cursor.execute(sqlcmd)
            sqlcmd = 'CREATE TABLE ' + table_name + ' ' + tblcols + ';'
            cursor.execute(sqlcmd)
        return 0
    except:
        print("Can't create table " + table_name)
        return -1

def insert_row(cursor, table_name, header, values):
    """
    This function inserts a new row in the given table into the currently selected database.

    :param cursor: database cursor
    :type cursor: object
    :param table_name: name of table to be created
    :type table_name: str
    :param header: a list describing the table column names and types
    :type header: list
    :param values: list of values to write into the
    :type values: list

    :return: Returns 0 if created successfully, -1 if an error is encountered
    """
    hdrstr = ''         # initialize a header string
    valstr = ''         # initialize a value string
    for i in header:
        hdrstr += i[0] + ','
    hdrstr = hdrstr.rstrip(',')

    for i in values:
        valstr += "'" + str(i) + "',"
    valstr = valstr.rstrip(',')

    try:
        cmd = 'INSERT INTO ' + table_name + ' (' + hdrstr + ') VALUES (' + valstr + ');'
        print(cmd)
        cursor.execute(cmd)
        return 0
    except:
        print("Can't insert row ...")
        return -1