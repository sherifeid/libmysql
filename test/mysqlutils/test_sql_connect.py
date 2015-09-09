from mysqlutils.primary import *

cur = connect_sql('elnamla.ddns.net', 3306, 'admin', 'sh18031978', 'SherifMedical')

alldbs = get_databases(cur)

for i in alldbs:
    print('Found database : ' + i)

select_database(cur, 'balalam')
select_database(cur, alldbs[0])

print(get_tables(cur))