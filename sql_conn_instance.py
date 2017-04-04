import pack.sql_connect as sql_conn
import fnmatch

host = '10.51.1.251'
user = 'app_etl_rw'
password = '%@BE0+g^gb'
database = 'spiderdb_test'
port = '3311'
charset = 'utf8'

sql_1 = sql_conn._MySQL(host, user, password, database, port, charset)
sql_1.sql_connect()
sql_fetch = 'select `id`, `name`, `area_code`, `reg_authority` from company_list' \
            ' where `area_code_status` = 1 limit 5000'
f = open('d:/my_data.txt', 'wt')
data = []
for row in sql_1.sql_fetch_rows(sql_fetch):
    try:
        dt = '{0:<5} >>>>> {1:*>30}{2:<6}{3:>30}\n'.format(str(row[0]), row[1], row[2], row[3])
        f.write(dt)
    except UnicodeEncodeError as err:
        print('fail to fetch data for id : %d' % (row[0]))

f.close()
