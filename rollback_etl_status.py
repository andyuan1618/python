# encoding:utf8

"""

rollback etl_status to 0 from src_tb_name like 'gs_shanghai_company'

"""
import pack.sql_connect as sql_conn
import logging.config
import sys
import logging
sys.path.append("../../../")
sys.path.append("e:/python")



def fetch_tb_name(db):
    tb_names = []
    sql_fetch_tb_name = "select `src` from config_site"
    for row in db.sql_fetch_rows(sql_fetch_tb_name):
        tb_names.append(row[0])
    return tb_names

def fetch_rows(db, src_tb_name):
    rows = []
    fetch_rows_stmt = "select `id`, `company_name` from " + src_tb_name + " where `etl_status` <> 0 limit 5000"
    for row in db.sql_fetch_rows(fetch_rows_stmt):
        rows.append([row[0], row[1]])
    return rows

def rollback_etl_status(db, src_tb_name, comp_id):
    update_stmt = "update " + src_tb_name + " set `etl_status` = 0 where `id` = %d" % comp_id
    db.sql_update_one(update_stmt)

def rollback():
    n = 0
    while True:
        if n >= 5:
            break
        try:
            # 连接数据库
            spiderdb_config = {"host":"10.51.1.251", "user":"app_etl_rw", "password":"%@BE0+g^gb", "database":"spiderdb_test", "port":3311, "charset":"utf8"}
            db_spider = sql_conn._MySQL(**spiderdb_config)
            db_spider.sql_connect()

            # 执行rollback
            src_tb_names = fetch_tb_name(db_spider)
            # src_tb_names = ['qixin_weixin_company', ]
            for src_tb_name in src_tb_names:
                while True:
                    rows = fetch_rows(db_spider, src_tb_name)
                    if len(rows) == 0:
                        logging.getLogger().info("No required data available for table: %s" % src_tb_name)
                        break
                    for row in rows:
                        try:
                            rollback_etl_status(db_spider, src_tb_name, row[0])
                            logging.getLogger().info("suc to rollback etl_status for id: %d , comp_name: %s in table: %s" % (row[0], row[1], src_tb_name))
                        except Exception as err:
                            logging.getLogger().error(err)
                            pass
        finally:
            try:
                db_spider.sql_close()
            except ConnectionError as e:
                logging.getLogger().error(e)
                pass
        n += 1


if __name__ == '__main__':
    logging.config.fileConfig("./config_log.txt")
    logging.getLogger().info("initial the etl task")

    rollback()

    logging.getLogger().info("end the etl task")
