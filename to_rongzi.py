# encoding:utf8

import sys
import logging
import logging.config
sys.path.append("e:/python")
sys.path.append("../../../")
import pack.sql_connect as sql_conn
import time
import re
import json
import os


# 获取source表名
def fetch_tb_name(db):
    tb_names = []
    sql_fetch_tb_name = "select `src` from config_site"
    for row in db.sql_fetch_rows(sql_fetch_tb_name):
        tb_names.append(row[0])
    return tb_names

# 获取注册地编码reg_area_code,获取失败返回None
def fetch_reg_area_code(db, reg_dept):
    sql_fetch_reg_area_code = "select `reg_gov_code` from sys_reg_gov where `reg_gov` = '%s' limit 1" % (reg_dept)
    fetched_reg_area_code = db.sql_fetch_one(sql_fetch_reg_area_code)
    reg_area_code = fetched_reg_area_code[0] if fetched_reg_area_code else None
    return reg_area_code

# 获取来源编码source_code
def fetch_source_code(db, tb_name):
    sql_fetch_source_code = "select `code` from config_site where `src` = '%s' limit 1" % (tb_name)
    src_code = db.sql_fetch_one(sql_fetch_source_code)[0]
    return src_code

# 获取source表名录
def fetch_name(db, tb_name):
    rows = []
    sql_fetch_name = "select `id`, `company_name`, `code_json`, `reg_dept` from " + tb_name + " where `etl_status` = 0 and `reg_dept` is not null and `code_json` is not null limit 5000"
    for row in db.sql_fetch_rows(sql_fetch_name):
        rows.append([row[0], row[1], row[2], row[3]])
    return rows

# 插入rongzi_corporation
def insert_into_rongzi_corporation(db, corp_name, uniform_code = None, register_code = None, organization_code = None, unknown_code = None):
    sql_insert_data = "insert into rongzi_corporation(`corporation_name`, `uniform_code`, `register_code`, `organization_code`, `unknown_code`) values (%s, %s, %s, %s, %s)"
    gene_corp_id = db.sql_insert_one(sql_insert_data, (corp_name, uniform_code, register_code, organization_code, unknown_code))
    return gene_corp_id

# 凭if_exists中码重复获取的fetched_corp_id，更新rongzi_corporation中code
def update_code(db, fetched_corp_id, code_dict):
    code_dict_to_update = {}
    sql_fetch_code = "select `uniform_code`, `register_code`, `organization_code`, `unknown_code` from rongzi_corporation where `id` = %d limit 1" % (fetched_corp_id)
    fetched_code = db.sql_fetch_one(sql_fetch_code)
    code_dict_to_update['uniform_code'] = fetched_code[0]
    code_dict_to_update['register_code'] = fetched_code[1]
    code_dict_to_update['organization_code'] = fetched_code[2]
    code_dict_to_update['unknown_code'] = fetched_code[3]
    for k in code_dict_to_update.keys():
        # print k, code_dict_to_update[k]
        if not code_dict_to_update[k] is None:
            del code_dict_to_update[k]
    # print code_dict_to_update
    for d in code_dict_to_update.keys():
        if d in code_dict.keys():
            if not code_dict[d] == '-' and not code_dict[d] is None:
                # print d, code_dict[d]
                code_dict_to_update[d] = code_dict[d]
                # print code_dict_to_update[d]
                sql_update_code = "update rongzi_corporation set `%s` = '%s' where `id` = %d" % (d, code_dict_to_update[d], fetched_corp_id)
                db.sql_update_one(sql_update_code)
                logging.getLogger().info("suc to update '%s':'%s' for fetched_corp_id: %d" % (d, code_dict_to_update[d], fetched_corp_id))

# 插入rongzi_rename
def insert_into_rongzi_rename(db, corp_id, corp_name, code):
    sql_insert_data = "insert into rongzi_rename(`corporation_id`, `corporation_name`, `code`) values (%d, '%s', '%s')" % (corp_id, corp_name, code)
    db.sql_insert_one(sql_insert_data)

# 插入rongzi_source
def insert_into_rongzi_source(db, corp_id, comp_id, src_tb_name, src_code, org_id, reg_area_code):
    sql_insert_data = "insert into rongzi_source(`corporation_id`, `company_id`, `company_table_name`, `source_code`, `org_id`, `reg_area_code`) values (%s, %s, %s, %s, %s, %s)"
    db.sql_insert_one(sql_insert_data, (corp_id, comp_id, src_tb_name, src_code, org_id, reg_area_code))

# 更新source表etl_status
def update_status(db, tb_name, status, company_id):
    sql_update_status = "update " + tb_name + " set `etl_status` = %d where `id` = %d" % (status, company_id)
    db.sql_update_one(sql_update_status)

# 判断code类型
def code_type(code):
    if not code:
        return None
    if re.search(r'-', code.strip(), re.I) and len(code.strip()) == 10:
        return 'organization_code'
    elif re.search("[\u4e00-\u9fa5]+", code.strip().decode('utf-8'), re.I):
        return 'register_code'
    elif len(code.strip()) == 18:
        return 'uniform_code'
    elif len(code.strip()) in (13, 14, 15):
        return 'register_code'
    else:
        return 'unknown_code'

# 判断记录重复情况
def if_exists(db, corp_name, code_dict, src_tb_name):
    rep_num = 0
    fetched_result = []
    rep_status = []
    for k in code_dict.keys():
        code_tp, code = k, code_dict[k]
        if code_dict[k] == '-' or code_dict[k] is None:
            continue
        sql_fetch = "select `id`, `corporation_name` from rongzi_corporation where `%s` = '%s' limit 1" % (code_tp, code)
        fetched_result = db.sql_fetch_one(sql_fetch)
        if not fetched_result:
            continue
        else:
            rep_num += 1
            break
    if rep_num == 0:
        rep_status = [0, '', '']    # 码不重复，插入rongzi_corporation & rongzi_source
    else:
        if fetched_result:
            fetched_corp_name = fetched_result[1]
            fetched_corp_id = fetched_result[0]
            if not fetched_corp_name == corp_name:
                rep_status = [1, fetched_corp_id, fetched_corp_name]    # 码重复，公司名不重复，插入rongzi_rename & rongzi_source
            else:
                sql_fetch_comp_tb_name_from_rongzi_source = "select `company_table_name` from rongzi_source where `corporation_id` = %d" % (fetched_corp_id)
                fetched_comp_tb_name = []
                for row in db.sql_fetch_rows(sql_fetch_comp_tb_name_from_rongzi_source):
                    fetched_comp_tb_name.append(row[0])
                if src_tb_name in fetched_comp_tb_name:
                    rep_status = [3, fetched_corp_id, fetched_corp_name]    # 码重复，公司名重复，来源重复，不插入任何表
                else:
                    rep_status = [2, fetched_corp_id, fetched_corp_name]    # 码重复，公司名重复，来源不重复，插入rongzi_source
    return rep_status


# 主任务
def init_etl():
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

            # 插入数据
            src_tb_names = fetch_tb_name(db_spider)
            # src_tb_names = ['tianyancha_web_company', 'qichacha_weixin_company', 'xizhi_web_company', 'qycxb_web_company']
            for src_tb_name in src_tb_names:
                reg_depts = set()
                path_name = 'd:\\fail to fetch reg_area_code\\'
                file_name = str(src_tb_name).split('_')[1] + '.txt'
                f = open(os.path.join(path_name, file_name), 'a')
                logging.getLogger().info("start etl data from table: '%s'" % src_tb_name)
                try:
                    src_code = fetch_source_code(db_spider, src_tb_name)
                except Exception as err2:
                    logging.getLogger().info("fail to fetch src_code for src_tb_name: %s" % src_tb_name)
                    logging.getLogger().error(err2)
                    continue
                while True:
                    rows = fetch_name(db_spider, src_tb_name)
                    if len(rows) == 0:
                        logging.getLogger().info("No required data available")
                        break
                    for row in rows:
                        try:
                            code_dict = json.loads(row[2], encoding='utf8')
                            reg_area_code = fetch_reg_area_code(db_rongzi, row[3])
                            if reg_area_code is None:
                                if row[3] not in reg_depts:
                                    str_row = str(row[3]) + '\n'
                                    f.write(str_row)
                                    f.flush()
                                    reg_depts.add(row[3])
                                else:
                                    pass
                            rep_status = if_exists(db_spider, row[1], code_dict, src_tb_name)
                            if rep_status[0] == 0:
                                corp_id = insert_into_rongzi_corporation(db_spider, row[1], **code_dict)
                                org_id = int('1' + str(corp_id))
                                insert_into_rongzi_source(db_spider, corp_id, row[0], src_tb_name, src_code, org_id, reg_area_code)
                                update_status(db_spider, src_tb_name, 1, row[0])
                            elif rep_status[0] == 1:
                                org_id = int('1' + str(rep_status[1]))
                                insert_into_rongzi_rename(db_spider, rep_status[1], row[1], row[2])
                                insert_into_rongzi_source(db_spider, rep_status[1], row[0], src_tb_name, src_code, org_id, reg_area_code)
                                update_code(db_spider, rep_status[1], code_dict)
                                update_status(db_spider, src_tb_name, 1, row[0])
                            elif rep_status[0] == 2:
                                org_id = int('1' + str(rep_status[1]))
                                insert_into_rongzi_source(db_spider, rep_status[1], row[0], src_tb_name, src_code, org_id, reg_area_code)
                                update_code(db_spider, rep_status[1], code_dict)
                                update_status(db_spider, src_tb_name, 1, row[0])
                            else:
                                update_code(db_spider, rep_status[1], code_dict)
                                update_status(db_spider, src_tb_name, 2, row[0])   # etl_status = 2 表示记录完全重复
                            logging.getLogger().info("suc to deal with data for id %d from table '%s' with rep_status %d" % (row[0], src_tb_name, rep_status[0]))
                        except Exception as err:
                            logging.getLogger().exception(err)
                            logging.getLogger().info("table: %s, id: %d" % (src_tb_name, row[0]))
                    time_sleep = 2
                    logging.getLogger().info("start to sleep for several secs")
                    time.sleep(time_sleep)
                f.close()
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

    init_etl()

    logging.getLogger().info("end the etl task")