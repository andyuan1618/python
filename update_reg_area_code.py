# encoding:utf8

"""
    更新rongzi_source表中reg_area_code

"""
import sys
sys.setdefaultencoding('utf-8')
import logging
import logging.config
sys.path.append("e:/python")
sys.path.append("../../../")
import pack.sql_connect as sql_conn


# 获取rongzi_source表中reg_area_code为空的行
def fetch_rows(db):
    rows = []
    stmt = "select `id`, `company_id`, `company_table_name` from rongzi_source where `reg_area_code` is null and `reg_status` = 0 limit 5000"
    for row in db.sql_fetch_rows(stmt):
        rows.append([row[0], row[1], row[2]])
    return rows

# 从src_tb_name反查注册地reg_dept
def fetch_reg_dept(db, src_tb_name, comp_id):
    stmt = "select `reg_dept` from " + src_tb_name + " where `id` = %d limit 1" % comp_id
    reg_dp = db.sql_fetch_one(stmt)
    reg_dept = reg_dp[0] if reg_dp is not None else None
    return reg_dept

# 根据工商注册地从rongzidb库sys_reg_gov表查出reg_area_code
def fetch_reg_area_code(db, reg_dp):
    stmt = "select `reg_gov_code` from sys_reg_gov where `reg_gov` = '%s' limit 1" % reg_dp
    reg_ar_code = db.sql_fetch_one(stmt)
    reg_area_code = reg_ar_code[0] if reg_ar_code is not None else None
    return reg_area_code

# 更新rongzi_source
def update_reg_area_code(db, reg_ar_code, id):
    stmt = "update rongzi_source set `reg_area_code` = %d where `id` = %d" % (reg_ar_code, id)
    db.sql_update_one(stmt)

# 更新rongzi_source表reg_status
def update_reg_status(db, status, id):
    stmt = "update rongzi_source set `reg_status` = %d where `id` = %d" % (status, id)
    db.sql_update_one(stmt)


# 主任务
def init_update():
    # 连接数据库
    spiderdb_config = {"host":"10.51.1.251", "user":"app_etl_rw", "password":"%@BE0+g^gb", "database":"spiderdb_test", "port":3311, "charset":"utf8"}
    rongzidb_config = {"host":"10.51.1.251", "user":"app_etl_rw", "password":"%@BE0+g^gb", "database":"rongzidb", "port":3311, "charset":"utf8"}
    db_spider = sql_conn._MySQL(**spiderdb_config)
    db_rongzi = sql_conn._MySQL(**rongzidb_config)
    db_spider.sql_connect()
    db_rongzi.sql_connect()

    # 执行update
    while True:
        try:
            rows = fetch_rows(db_spider)
            if len(rows) == 0:
                logging.getLogger().info("No required data available")
                break
            for row in rows:
                try:
                    reg_dept = fetch_reg_dept(db_spider, row[2], row[1])
                    if reg_dept is not None:
                        try:
                            reg_area_code = fetch_reg_area_code(db_rongzi, reg_dept)
                            if reg_area_code is not None:
                                update_reg_area_code(db_spider, reg_area_code, row[0])
                                logging.getLogger().info("Suc to update reg_area_code for id: %s" % (row[0]))
                                update_reg_status(db_spider, 1, row[0])     # 1代表更新成功
                            else:
                                logging.getLogger().info("Fail to fetch reg_area_code for id: %s" % (row[0]))
                                update_reg_status(db_spider, 2, row[0])     # 2代表获取reg_area_code失败
                        except Exception as err:
                            logging.getLogger().info("Fetch reg_area_code error for id: %s" % (row[0]))
                            logging.getLogger().error(err)
                            update_reg_status(db_spider, 4, row[0])  # 4代表reg_dept非法
                    else:
                        logging.getLogger().info("Fail to fetch reg_dept for id: %s" % (row[0]))
                        update_reg_status(db_spider, 3, row[0])  # 3代表获取reg_dept失败
                except Exception as err:
                    logging.getLogger().info("Fetch reg_dept error for id: %s" % (row[0]))
                    logging.getLogger().error(err)
        finally:
            try:
                db_spider.sql_close()
                db_rongzi.sql_close()
            except Exception as e:
                logging.getLogger().info(e)
                pass


if __name__ == '__main__':
    logging.config.fileConfig("./config_log.txt")
    logging.getLogger().info("Initial the etl task")

    init_update()

    logging.getLogger().info("End the etl task")