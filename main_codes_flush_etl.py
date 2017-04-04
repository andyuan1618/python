# -*- coding:utf-8 -*-

import thread
import logging.config
import json
import threading
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append("../../../")
import app.common.db as db
import app.common.code_classify as classify


def main():
    cnxpool = db.sql_connect("./default.ini", "spider_con_config")
    try:
        cnx = cnxpool.get_connection()
        shanghai_web_site = db.sql_fetch_one(cnx, "select value from common_config where `key`='shanghai_web_site'", )[
            0]
        fujian_web_site = db.sql_fetch_one(cnx, "select value from common_config where `key`='fujian_web_site'", )[0]
        hebei_web_site = db.sql_fetch_one(cnx, "select value from common_config where `key`='hebei_web_site'", )[0]
        hunan_web_site = db.sql_fetch_one(cnx, "select value from common_config where `key`='hunan_web_site'", )[0]
        yunnan_web_site = db.sql_fetch_one(cnx, "select value from common_config where `key`='yunnan_web_site'", )[0]
        guangdong_weixin_site = \
            db.sql_fetch_one(cnx, "select value from common_config where `key`='guangdong_weixin_site'", )[0]
        qixin_weixin_site = db.sql_fetch_one(cnx, "select value from common_config where `key`='qixin_weixin_site'", )[
            0]
    except Exception as err:
        logging.getLogger().info(err)
    cnx.close()
    threads = [
        threading.Thread(target=gs_web_flush,
                         args=(cnxpool.get_connection(), 'gs_shanghai_company', shanghai_web_site)),
        threading.Thread(target=gs_web_flush, args=(cnxpool.get_connection(), 'gs_fujian_company', fujian_web_site)),
        threading.Thread(target=gs_web_flush, args=(cnxpool.get_connection(), 'gs_hebei_company', hebei_web_site)),
        threading.Thread(target=gs_web_flush, args=(cnxpool.get_connection(), 'gs_hunan_company', hunan_web_site)),
        threading.Thread(target=gs_web_flush, args=(cnxpool.get_connection(), 'gs_yunnan_company', yunnan_web_site)),
        threading.Thread(target=gs_guangdong_weixin_flush,
                         args=(cnxpool.get_connection(), 'gs_guangdong_company', guangdong_weixin_site)),
        threading.Thread(target=gs_qixin_weixin_flush,
                         args=(cnxpool.get_connection(), 'qixin_weixin_company', qixin_weixin_site))]

    for t in threads:
        t.start()


def gs_web_flush(cnx, company_table, site):
    while True:
        logging.getLogger().info(" round starting ")
        sql_string = " SELECT id, data_table_name FROM " + company_table + " WHERE parse_status =1 limit 200 "
        data = db.sql_fetch_rows(cnx, sql_string)
        for item in data:
            company_id, data_table_name = item[0], item[1]
            logging.getLogger().info(
                " site: %s / data_table_name : %s / company_id : %s " % (site, data_table_name, company_id))
            sql = " SELECT value FROM " + data_table_name + " WHERE site = %s and company_id = %s and key_desc = %s "
            try:
                gsgs = json.loads(db.sql_fetch_one(cnx, sql, [site, company_id, 'gsgs'])[0])
                code, reg_dept = None, None
                code = gsgs[u'登记信息'][u'基本信息'][u'注册号/'].strip()
                type_ = classify.code_type(code)
                res = {type_: code}
                reg_dept_temp = gsgs[u'登记信息'][u'基本信息'][u'登记机关']
                if reg_dept_temp is not None:
                    reg_dept = reg_dept_temp.strip()
                db.sql_update(cnx,
                              " UPDATE " + company_table + " SET  code_json = %s , reg_dept = %s, parse_status = %s"
                                                           " WHERE id = %s ",
                              [json.dumps(res), reg_dept, 2, company_id])
                logging.getLogger().info(" operations ends, site: %s / common_data_name : %s / company_id : %s " % (
                    site, data_table_name, company_id))
            except Exception as err:
                db.sql_update(cnx, " UPDATE " + company_table + " SET parse_status = %s WHERE id = %s ",
                              [5, company_id])
                logging.getLogger().info(err)
                continue

    cnx.close()


def gs_guangdong_weixin_flush(cnx, company_table, site):
    while True:
        logging.getLogger().info(" round starting ")
        sql_string = " SELECT id, data_table_name FROM " + company_table + " WHERE parse_status =1 limit 200 "
        data = db.sql_fetch_rows(cnx, sql_string)
        for item in data:
            company_id, data_table_name = item[0], item[1]
            logging.getLogger().info(
                " site:%s / data_table_name : %s / company_id : %s " % (site, data_table_name, company_id))
            sql = " SELECT value FROM " + data_table_name + " WHERE site = %s and company_id = %s and key_desc = %s "
            try:
                gsgs = json.loads(db.sql_fetch_one(cnx, sql, [site, company_id, 'gsgs'])[0])
                code, reg_dept = None, None
                if u'regNO' in gsgs[u'result'][u'RegInfo'][u'BaseInfo'].keys():
                    code = gsgs[u'result'][u'RegInfo'][u'BaseInfo'][u'regNO'].strip()
                if u'regOrg' in gsgs[u'result'][u'RegInfo'][u'BaseInfo'].keys():
                    reg_dept = gsgs[u'result'][u'RegInfo'][u'BaseInfo'][u'regOrg'].strip()
                if (code is None) and (reg_dept is None):
                    db.sql_update(cnx, " UPDATE " + company_table + " SET parse_status = %s WHERE id = %s ",
                                  [5, company_id])
                    logging.getLogger().info("register code and register department info are empty")
                    continue
                type_ = classify.code_type(code)
                res = {type_: code}
                db.sql_update(cnx,
                              " UPDATE " + company_table + " SET  code_json = %s , reg_dept = %s, parse_status = %s"
                                                           " WHERE id = %s ",
                              [json.dumps(res), reg_dept, 2, company_id])
                logging.getLogger().info(" operations ends, site: %s / common_data_name : %s / company_id : %s " % (
                    site, data_table_name, company_id))
            except Exception as err:
                db.sql_update(cnx, " UPDATE " + company_table + " SET parse_status = %s WHERE id = %s ",
                              [5, company_id])
                logging.getLogger().info(err)
                continue
    cnx.close()


def gs_qixin_weixin_flush(cnx, company_table, site):
    while True:
        logging.getLogger().info(" round starting ")
        sql_string = " SELECT id, data_table_name FROM " + company_table + " WHERE parse_status =1 limit 200 "
        data = db.sql_fetch_rows(cnx, sql_string)
        for item in data:
            company_id, data_table_name = item[0], item[1]
            logging.getLogger().info(
                "  site:%s / data_table_name : %s / company_id : %s " % (site, data_table_name, company_id))
            sql = " SELECT value FROM " + data_table_name + " WHERE  site = %s and company_id = %s and key_desc = %s "
            try:
                basicinfo = json.loads(db.sql_fetch_one(cnx, sql, [site, company_id, 'basicinfo'])[0])
                register_code, uniform_code, organization_code, reg_dept = None, None, None, None
                if u'reg_no' in basicinfo.keys():
                    register_code = basicinfo[u'reg_no'].strip()
                if u'credit_no' in basicinfo.keys():
                    uniform_code = basicinfo[u'credit_no'].strip()
                if u'org_no' in basicinfo.keys():
                    organization_code = basicinfo[u'org_no'].strip()
                if u"belong_org" in basicinfo.keys():
                    reg_dept = basicinfo[u'belong_org'].strip()
                if (register_code is None) and (uniform_code is None) and (organization_code is None) and (
                            reg_dept is None):
                    db.sql_update(cnx, " UPDATE " + company_table + " SET parse_status = %s WHERE id = %s ",
                                  [5, company_id])
                    continue
                res = {u"register_code": register_code, u'uniform_code': uniform_code,
                       u'organization_code': organization_code}
                db.sql_update(cnx,
                              " UPDATE " + company_table + " SET  code_json = %s , reg_dept = %s, parse_status = %s"
                                                           " WHERE id = %s ",
                              [json.dumps(res), reg_dept, 2, company_id])
                logging.getLogger().info(" operations ends, site: %s / common_data_name : %s / company_id : %s " % (
                    site, data_table_name, company_id))
            except Exception as err:
                db.sql_update(cnx, " UPDATE " + company_table + " SET parse_status = %s WHERE id = %s ",
                              [5, company_id])
                logging.getLogger().info(err)
                continue

    cnx.close()


if __name__ == "__main__":
    logging.config.fileConfig("./conf_log.txt")
    main()
