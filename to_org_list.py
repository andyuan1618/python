# encoding:utf8

import sys
sys.path.append("../../../")
sys.path.append("e:/python")
import pack.sql_connect as sql_conn
import logging
import logging.config
import time


def fetch_rows(db):
    rows = []
    fetch_rows_stmt = "select `id`, `corporation_name`, `uniform_code`, `register_code`, `organization_code`, `unknown_code` from rongzi_corporation where `etl_status` = 0 limit 5000"
    for row in db.sql_fetch_rows(fetch_rows_stmt):
        rows.append([row[0], row[1], row[2], row[3], row[4], row[5]])
    return rows

def insert_into_org_list(db, org_id, corp_name, uniform_code, register_code, organization_code, unknown_code):
    insert_stmt = "insert into org_list(`org_id`, `org_name`, `unq_symbol`, `reg_symbol`, `org_symbol`, `oth_symbol`) values(%s, %s, %s, %s, %s, %s)"
    db.sql_insert_one(insert_stmt, (org_id, corp_name, uniform_code, register_code, organization_code, unknown_code))

def update_etl_status(db, status, corp_id):
    update_stmt = "update rongzi_corporation set `etl_status` = %s where `id` = %s"
    db.sql_update_one(update_stmt, (status, corp_id))

def init_etl_task():
    n = 0
    while True:
        if n >= 5:
            break
        try:
            # 连接数据库
            spiderdb_config = {"host":"10.51.1.251", "user":"app_etl_rw", "password":"%@BE0+g^gb", "database":"spiderdb_test", "port":3311, "charset":"utf8"}
            rongzidb_config = {"host":"10.51.1.251", "user":"app_etl_rw", "password":"%@BE0+g^gb", "database":"rongzidb", "port":3311, "charset":"utf8"}
            db_spider = sql_conn._MySQL(**spiderdb_config)
            db_rongzi = sql_conn._MySQL(**rongzidb_config)
            db_spider.sql_connect()
            db_rongzi.sql_connect()

            # 执行etl
            while True:
                rows = fetch_rows(db_spider)
                if len(rows) == 0:
                    logging.getLogger().info("No required data available")
                    break
                for row in rows:
                    try:
                        org_id = int('1' + str(row[0]))
                        insert_into_org_list(db_rongzi, org_id, row[1], row[2], row[3], row[4], row[5])
                        update_etl_status(db_spider, 1, row[0])
                        logging.getLogger().info("suc to etl data into org_list for corp_id: %d with corp_name: %s" % (row[0], row[1]))
                    except Exception as err:
                        update_etl_status(db_spider, 2, row[0])
                        logging.getLogger().error(err)
                # time_sleep = 3
                # logging.getLogger().info("start to sleep for several secs")
                # time.sleep(time_sleep)
        finally:
            try:
                db_spider.sql_close()
                db_rongzi.sql_close()
            except Exception as e:
                logging.getLogger().info(e)
                pass

        n += 1

if __name__ == '__main__':
    logging.config.fileConfig("./config_log.txt")
    logging.getLogger().info("initial the etl task")

    init_etl_task()

    logging.getLogger().info("end the etl task")
