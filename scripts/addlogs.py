__author__ = 'Sherif Eid'
"""
This script takes a directory as an argument, parses the included csv files
(assuming they follow the specific header order) then appends the values to
the database
"""

import glob
from mysqlutils.primary import *
import time

def usage():
    print('-----------------------------------------------------------------------------------------')
    print('Usage:')
    print('python addlogs.py Directory')
    print('    Directory   : location of directory containing csv log files')
    print('-----------------------------------------------------------------------------------------')
    sys.exit(0)

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# init variables

dbserver = 'elnamla.ddns.net'
dbport = 3306
dbun = 'admin'
dbpasswd = 'sh18031978'
dbname = 'SherifMedical'
meastable = 'IOX'

# describe table header here
tblheader = []
tblheader.append(['year', 'varchar(255)'])
tblheader.append(['month', 'varchar(255)'])
tblheader.append(['day', 'varchar(255)'])
tblheader.append(['hour', 'varchar(255)'])
tblheader.append(['min', 'varchar(255)'])
tblheader.append(['sec', 'varchar(255)'])
tblheader.append(['heartrate', 'varchar(255)'])
tblheader.append(['spo2', 'varchar(255)'])
tblheader.append(['pi', 'varchar(255)'])
tblheader.append(['respiration', 'varchar(255)'])

if len(sys.argv) != 2:
    # arguments are not correct
    usage()
else:
    cwd = sys.argv[1]
    print(bcolors.OKGREEN + 'Pulling CSV files from directory : ' + cwd + bcolors.ENDC)
    files = glob.glob(cwd + '/*.csv')

    # connect to database
    cur = connect_sql(dbserver, dbport, dbun, dbpasswd, dbname)
    select_database(cur, dbname)

    #cur.execute("INSERT INTO IOX (year,month,day,hour,min,sec,heartrate,spo2,pi,respiration) VALUES ('2015','09','01','01','11','11','62','90','4.2','16');")
    #time.sleep(10)


    # create the table if it's not there
    create_table(cur,meastable,tblheader,safe=True)

    for i in files:
        print(bcolors.OKBLUE + 'Processing file : ' + i + bcolors.ENDC)

        try :
            alllines = file_len(i)
            print(bcolors.OKBLUE + 'processing ' + str(alllines) + ' lines...' + bcolors.ENDC)
            with open(i,'r') as f:
                curline = 1
                next(f)
                for line in f:
                    # parse the line
                    _line = line.strip('\n')
                    linesplit1 = _line.split(' ')
                    date = linesplit1[0].split('-')
                    linesplit2 = linesplit1[1].split(',')
                    time = linesplit2[0].split(':')
                    hr = linesplit2[1]
                    spo2 = linesplit2[2]
                    pi = linesplit2[3]
                    resp = linesplit2[4]
                    # add line to
                    values=[date[0],date[1],date[2],time[0],time[1],time[2],hr,spo2,pi,resp]
                    insert_row(cur,meastable,tblheader,values)
                    #print()
                    #print('Current DB is ' + get_currentdb(cur))

                    # update progress bar indicator
                    curline += 1
                    progress = 100*curline/alllines
                    sys.stdout.write("\r%d%% complete" % round(100*curline/alllines))
                    sys.stdout.flush()

            print()

        except :
            print(bcolors.FAIL + 'Unable to open CSV file : ' + i + bcolors.ENDC)
