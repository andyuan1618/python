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
    stmt = "select * from msapplications"
    for row in db.sql_fetch_rows(stmt):
        rows.append([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                     row[8], row[9], row[10], row[11]])
    return rows


def if_exists(db, appid):
    stmt = "select `Principal` from msapplications where `appid` = %s limit 1"
    row = db.sql_fetch_one(stmt, appid)
    if row is not None:
        repeat = 1
    else:
        repeat = 0
    return repeat


def insert_rows(db, appid=None, accountid=None, principal=None, repayments=None, downpayment=None,
                status=None, channel=None, installmentstartedon=None, moneytransferredon=None,
                isdeleted=None, deletedon=None):
    stmt = "insert into msapplications(`AppId`, `AccountId`, `Principal`, `Repayments`, `Downpayment`, `Status`, " \
           "`Channel`, `InstallmentStartedOn`, `MoneyTransferredOn`, `IsDeleted`, `DeletedOn`) " \
           "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    db.sql_insert_one(stmt, [appid, accountid, principal, repayments, downpayment, status, channel,
                             installmentstartedon, moneytransferredon, isdeleted, deletedon])


def update_rows(db, accountid, principal, repayments, downpayment, status, channel, installmentstartedon,
                moneytransferredon, isdeleted, deletedon, appid):
    stmt = "update msapplications set `AccountId` = %s, `Principal` = %s, `Repayments` = %s, `Downpayment` = %s, " \
           "`Status` = %s, `Channel` = %s, `InstallmentStartedOn` = %s, `MoneyTransferredOn` = %s, `IsDeleted` = %s, " \
           "`DeletedOn` = %s where `AppId` = %s"
    db.sql_update_one(stmt, [accountid, principal, repayments, downpayment, status, channel, installmentstartedon,
                             moneytransferredon, isdeleted, deletedon, appid])


def main():
    global db_loan, db_target
    try:
        # 连接数据库

        # source库
        db_loan_config = {"host": "rr-bp110962n4ob6h1a5.mysql.rds.aliyuncs.com", "user": "dw",
                          "password": "+$h#6dTIFner7rEU", "database": "prod_merchant_loan", "port": 3806,
                          "charset": "utf8"}
        db_loan = sql_conn._MySQL(**db_loan_config)
        db_loan.sql_connect()

        # target库
        db_target_config = {"host": "120.26.10.192", "user": "integrate", "password": "Bigdata1234!",
                            "database": "prod_merchant_loan", "port": 3306, "charset": "utf8"}
        db_target = sql_conn._MySQL(**db_target_config)
        db_target.sql_connect()

        # 启动etl
        while True:
            for row in fetch_rows(db_loan):
                logging.getLogger().info("%s %s %s %s" % (row[1], row[3], row[8], row[11]))
                try:
                    repeat = if_exists(db_target, row[1])
                    if repeat == 0:
                        logging.getLogger().info("No repeat for id: %d" % (row[0]))
                        insert_rows(db_target, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                    row[9], row[10], row[11])
                    elif repeat == 1:
                        logging.getLogger().info("Yes repeat for id: %d" % (row[0]))
                        update_rows(db_target, row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                    row[9], row[10], row[11], row[1])
                    else:
                        pass
                    logging.getLogger().info("Suc to insert/update record with id %d" % (row[0]))
                except Exception as e:
                    logging.getLogger().info("Fail to insert/update record with id %d" % (row[0]))
                    print e
            sleep_sec = 300
            logging.getLogger().info("Start to sleep for 300 secs")
            time.sleep(sleep_sec)
    except Exception as e:
        print e
    finally:
        try:
            db_loan.sql_close()
            db_target.sql_close()
        except Exception as e:
            logging.getLogger().info(e)
            pass


if __name__ == '__main__':
    logging.config.fileConfig('./config_log.txt')
    logging.getLogger().info("Initial main task")

    main()

    logging.getLogger().info("End main task")










