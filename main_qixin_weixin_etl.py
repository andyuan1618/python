# coding=utf-8
import sys
import datetime
import re
import io
from decimal import *
import mysql.connector
import ConfigParser
import json
import logging.config

reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../")
from app.common.utils import address_code_parsing
import app.common.db as db


def main():
    logging.config.fileConfig("./conf_log.txt")
    cnxpool = db.sql_connect("default.ini", "spider_con_config")
    source_code = 50001
    while True:
        cnx = cnxpool.get_connection()
        data = db.sql_fetch_rows(cnx, '''select id, company_table_name, company_id, corporation_id, reg_area_code,
            source_code from rongzi_source where source_code = %s and reg_area_code is not null and etl_status = 0  limit 200 ''', [source_code, ])
        for item in data:
            item_id, company_table_name, company_id, corporation_id, reg_area_code, source_code = item[0], item[1], item[2], item[3], item[4], item[5]
            logging.getLogger().info(" Begin to operate on ID: %s " % (item_id,))
            org_id = int(str(1) + str(corporation_id))  # org_id 生产逻辑
            try:
                data_table_name = db.sql_fetch_one(cnx, " select data_table_name from " + company_table_name + " where id = %s", [company_id])[0]
                site = db.sql_fetch_one(cnx, " select site from config_site where code = %s ", [source_code])[0]
                key_values = db.sql_fetch_rows(cnx, " select key_desc, value from " + data_table_name + " where company_id = %s and site = %s ",
                                               [company_id, site])
                sql_groups = []
                params_groups = []
                for i in key_values:
                    key, value = i[0], i[1]
                    data = json.loads(value)
                    src_cd = source_code  # NOT NULL
                    org_reg_id = str(reg_area_code) + str(src_cd) + str(org_id)  # NOT NULL
                    org_name = None
                    if key == u'basicinfo':
                        '''
                        基本信息
                        '''
                        keys = data.keys()
                        org_name = data['name']  # NOT NULL
                        org_type, leg_reps, reg_cap, reg_cap_curr = None, None, None, None
                        if 'econ_kind' in keys:
                            org_type = trim_dict(data, 'econ_kind')
                        if 'oper_name' in keys:
                            leg_reps = trim_dict(data, 'oper_name')
                        if 'regist_capi' in keys:
                            temp = trim_dict(data, 'regist_capi')
                            if temp is not None:
                                splits = temp.replace(u'&nbsp;', u'').replace(u'万元', u'万').split(u'万')
                                reg_cap = Decimal.from_float(float(splits[0].replace(u',', u'')))
                                reg_cap_curr = splits[1]
                        found_date, reg_address = None, None
                        if 'term_start' in keys:
                            temp = trim_dict(data, 'term_start')
                            found_date = date_common_datetoint(temp) if temp is not None else None
                        if 'addresses' in keys:
                            if len(data['addresses']) != 0:
                                reg_address = data['addresses'][0]['address']
                        country, city = u"中国", None
                        if reg_address is not None:
                            city = address_code_parsing(reg_address, reg_area_code, cnx)
                        else:
                            city = code_parsing(reg_area_code, cnx)
                        oper_begin_date, oper_end_date, oper_scope, reg_gov, = None, None, None, None
                        if 'start_date' in keys:
                            temp = trim_dict(data, 'start_date')
                            oper_begin_date = date_common_datetoint(temp) if temp is not None else None
                        if 'end_date' in keys:
                            temp = trim_dict(data, 'end_date')
                            oper_end_date = date_common_datetoint(temp) if temp is not None else None
                        if 'scope' in keys:
                            oper_scope = trim_dict(data, 'scope')
                        if 'belong_org' in keys:
                            reg_gov = trim_dict(data, 'belong_org')
                        reg_gov_cd = reg_area_code
                        appr_date, reg_status, reg_status_cd = None, None, None
                        if 'check_date' in keys:
                            temp = trim_dict(data, 'check_date')
                            appr_date = date_common_datetoint(temp) if temp is not None else None
                        if 'status' in keys:
                            temp = trim_dict(data, 'status')
                            reg_status = temp.split(u'（')[0] if temp is not None else None
                        if reg_status is not None:
                            temp = db.sql_fetch_one(cnx, " select param_code from rongzidb.sys_param where value_name = %s ", [reg_status])
                            if temp is not None:
                                reg_status_cd = temp[0]
                        sql = '''
                            INSERT INTO rongzidb.org_reg_info(org_reg_id, src_cd, org_id, org_name, org_type,
                            leg_reps, reg_cap, reg_cap_curr, found_date, reg_address,
                            country, city, oper_begin_date, oper_end_date, oper_scope,
                            reg_gov, reg_gov_cd, appr_date, reg_status,reg_status_cd ) VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                              '''
                        sql_groups.append(sql)
                        params_groups.append((org_reg_id, src_cd, org_id, org_name, org_type,
                                              leg_reps, reg_cap, reg_cap_curr, found_date, reg_address,
                                              country, city, oper_begin_date, oper_end_date, oper_scope,
                                              reg_gov, reg_gov_cd, appr_date, reg_status, reg_status_cd))
                        '''
                        企业主要人员
                        '''
                        staffs = data["employees"]
                        sql = '''
                            INSERT INTO rongzidb.org_staff(org_reg_id, src_cd,org_id, person_name, position, end_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            '''
                        if staffs is not None:
                            for staff in staffs:
                                person_name = trim_dict(staff, 'name')
                                position = trim_dict(staff, 'job_title')
                                end_date = 19900101  # NOT NULL
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, person_name, position, end_date))

                        '''
                        分支机构
                        '''
                        sql = '''
                            INSERT INTO rongzidb.org_branch(org_reg_id, src_cd, org_id, org_branch_name, org_branch_id, src_json)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            '''
                        branches = data["branches"]
                        if branches is not None:
                            for branch in branches:
                                org_branch_name = trim_dict(branch, 'name')
                                if org_branch_name is None:
                                    continue
                                sql_ = " select org_id from rongzidb.org_list where org_name = %s "
                                temp = db.sql_fetch_one(cnx, sql_, [org_branch_name])
                                org_branch_id, src_json = None, None
                                if temp is None:
                                    pass
                                else:
                                    org_branch_id = temp[0]
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, org_branch_name, org_branch_id, src_json))
                    elif key == u'changeinfo':
                        '''
                        企业信息变更
                        '''
                        chginfo = data['changerecords']
                        sql = '''
                                INSERT INTO rongzidb.org_reg_chg(org_reg_id, src_cd,org_id, chg_proj, pre_chg, post_chg, chg_date)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            '''
                        if chginfo is not None:
                            for k in chginfo:
                                chg_proj, pre_chg, post_chg, chg_date = None, None, None, None
                                keys = k.keys()
                                if "change_item" in keys:
                                    chg_proj = trim_dict(k, 'change_item')
                                if "before_content" in keys:
                                    pre_chg = trim_dict(k, 'before_content')
                                if "after_content" in keys:
                                    post_chg = trim_dict(k, 'after_content')
                                if "change_date" in keys:
                                    temp = trim_dict(k, 'change_date')
                                    chg_date = date_common_datetoint(temp) if temp is not None else None
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, chg_proj, pre_chg, post_chg, chg_date))
                # 开启事务，进行多表插入
                try:
                    cnx.autocommit = False
                    cursor = cnx.cursor()
                    for j in range(0, len(sql_groups)):
                        sql = sql_groups[j]
                        params = params_groups[j]
                        cursor.execute(sql, params)
                    cnx.commit()
                except Exception, err:
                    cnx.rollback()
                    logging.getLogger().exception(err)
                    raise Exception
                finally:
                    cursor.close()
                    cnx.autocommit = True
                db.sql_update(cnx, " update rongzi_source set etl_status = 1 where id = %s ", [item_id])
                cnx.commit()
                logging.getLogger().info(" Insert Succeed ")
            except Exception, err:
                logging.getLogger().info(err)
                db.sql_update(cnx, " update rongzi_source set etl_status = 2 where id = %s ", [item_id])
                cnx.commit()
                logging.getLogger().info(" Insert Failed")
        cnx.close()


def trim_dict(dict_, key_):
    if (dict_[key_]) is None or (dict_[key_].strip() == '') or (dict_[key_].strip() == u'-') or (dict_[key_].strip() == u'暂无'):
        return None
    return dict_[key_].strip()


def date_strtoint(datestr):
    return int(datetime.datetime.strptime(datestr, u'%Y年%m月%d日').strftime(u'%Y%m%d'))


def code_parsing(area_code, cnx):
    r = None
    b = re.match('\d{2}0{4}', str(area_code))
    if b is None:
        r = int(str(area_code)[0:4] + "00")
    else:
        r = area_code
    return r


def date_common_datetoint(datestr):
    r = None
    if datestr.__contains__(u'月'):
        r = date_strtoint(datestr)
    elif datestr.__contains__(u','):
        datestr = datestr.replace(u'AM', u'').replace(u'PM', u'').strip()
        r = int(datetime.datetime.strptime(datestr, u'%b %d, %Y %H:%M:%S').strftime(u'%Y%m%d'))
    elif datestr.__contains__(u'T'):
        r = int(datetime.datetime.strptime(datestr, u'%Y-%m-%dT%H:%M:%S').strftime(u'%Y%m%d'))
    else:
        r = int(datetime.datetime.strptime(datestr, u'%Y-%m-%d').strftime(u'%Y%m%d'))
    return r


if __name__ == '__main__':
    main()
