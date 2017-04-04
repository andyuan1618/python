#!/usr/bin/python
# -*- coding: utf8 -*-
import logging
import logging.config
import time
import sql_connect as sql_conn
import sys
sys.path.append('/home/bigdata/stonehange/tools/python')
sys.path.append("../../../")


def fetch_rows(db):
    rows = []
    stmt = "select * from pdl_appfp_relation"
    for row in db.sql_fetch_rows(stmt):
        rows.append([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]])
    return rows


def if_exists(db, pid):
    stmt = "select `AppId` from pdl_appfp_relation where `id` = %s"
    row = db.sql_fetch_one(stmt, pid)
    if row is not None:
        repeat = 1
    else:
        repeat = 0
    return repeat


def insert_rows(db, pid=None, appid=None, accountid=None, fundid=None, financeproductid=None, principal=None,
                loandays=None ,moneytransferredon=None, isdeleted=None, deletedon=None):
    stmt = "insert into pdl_appfp_relation(`id`, `AppId`, `AccountId`, `fundid`, `financeproductid`, `Principal`," \
           " `loandays`, `MoneyTransferredOn`, `IsDeleted`, `DeletedOn`)" \
           " values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    db.sql_insert_one(stmt, [pid, appid, accountid, fundid, financeproductid, principal, loandays, moneytransferredon,
                             isdeleted, deletedon])


def update_rows(db, pid, appid, accountid, fundid, financeproductid, principal, loandays, moneytransferredon,
                isdeleted, deletedon):
    stmt = "update pdl_appfp_relation set `appid` = %s, `AccountId` = %s, `fundid` = %s, `financeproductid` = %s, " \
           "`Principal` = %s, `loandays` = %s, `MoneyTransferredOn` = %s, `IsDeleted` = %s, `DeletedOn` = %s " \
           "where `id` = %s"
    db.sql_update_one(stmt, [appid, accountid, fundid, financeproductid, principal, loandays,
                             moneytransferredon, isdeleted, deletedon, pid])


def main():
    global db_credit, db_target
    try:
        # 连接数据库

        # source库
        db_credit_config = {"host": "rr-bp1vbih632ag258n2.mysql.rds.aliyuncs.com", "user": "bigdata",
                          "password": "4i7vRXAsRULQJHvB", "database": "prod_pdl_credit", "port": 3306,
                          "charset": "utf8"}
        db_credit = sql_conn._MySQL(**db_credit_config)
        db_credit.sql_connect()

        # target库
        db_target_config = {"host": "120.26.10.192", "user": "integrate", "password": "Bigdata1234!",
                            "database": "prod_pdl_credit", "port": 3306, "charset": "utf8"}
        db_target = sql_conn._MySQL(**db_target_config)
        db_target.sql_connect()

        # 启动etl
        while True:
            for row in fetch_rows(db_credit):
                logging.getLogger().info("%s %s %s %s" % (row[1], row[3], row[6], row[9]))
                try:
                    repeat = if_exists(db_target, row[0])
                    if repeat == 0:
                        logging.getLogger().info("No repeat for id: %s" % (row[0]))
                        insert_rows(db_target, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                    row[9])
                    elif repeat == 1:
                        logging.getLogger().info("Yes repeat for id: %s" % (row[0]))
                        update_rows(db_target, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                    row[9])
                    else:
                        pass
                    logging.getLogger().info("Suc to insert/update record with id %s" % (row[0]))
                except Exception as e:
                    logging.getLogger().info("Fail to insert/update record with id %s" % (row[0]))
                    print e
            sleep_sec = 300
            logging.getLogger().info("Start to sleep for 300 secs")
            time.sleep(sleep_sec)
    except Exception as e:
        print e
    finally:
        try:
            db_credit.sql_close()
            db_target.sql_close()
        except Exception as e:
            logging.getLogger().info(e)
            pass


if __name__ == '__main__':
    logging.config.fileConfig('./config_log.txt')
    logging.getLogger().info("Initial main task")

    main()

    logging.getLogger().info("End main task")










