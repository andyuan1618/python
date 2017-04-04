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
import time

reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../")
from app.common.utils import address_code_parsing
import app.common.db as db


def main():
    logging.config.fileConfig("./conf_log.txt")
    cnxpool = db.sql_connect("default.ini", "spider_con_config")
    # source_codes = [90310, 90350, 90130, 90430, 90530]
    source_codes = [90310, 90310, 90310, 90310, 90310]
    while True:

        cnx = cnxpool.get_connection()
        items = db.sql_fetch_rows(cnx, '''select id, company_table_name, company_id, corporation_id, reg_area_code,
            source_code from rongzi_source where source_code in ( %s, %s, %s, %s, %s) and etl_status = 0 limit 1 ''', source_codes)
        if len(items) == 0:
            time.sleep(10)
            continue
        for item in items:

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
                    if key == 'gsgs':  # 工商公示
                        '''
                            <1> 基本信息
                        '''
                        basic = data[u'登记信息'][u'基本信息']

                        org_name = basic[u'名称'].strip()  # NOT NULL
                        org_type = trim_dict(basic, u'类型')
                        leg_reps = None
                        if u'法定代表人' in basic.keys():
                            leg_reps = trim_dict(basic, u'法定代表人')
                        elif u'经营者' in basic.keys():
                            leg_reps = trim_dict(basic, u'经营者')
                        elif u'负责人' in basic.keys():
                            leg_reps = trim_dict(basic, u'负责人')
                        reg_cap, reg_cap_curr = None, None
                        if u'注册资本' in basic.keys():
                            splits = trim_dict(basic, u'注册资本').split(u'万') if trim_dict(basic, u'注册资本') is not None else None
                            reg_cap = Decimal.from_float(float(splits[0])) if splits is not None else None
                            reg_cap_curr = splits[1] if splits is not None else None
                        found_date = None
                        if u'成立日期' in basic.keys():
                            found_date = date_strtoint(trim_dict(basic, u'成立日期')) if trim_dict(basic, u'成立日期') is not None else None
                        elif u'注册日期' in basic.keys():
                            found_date = date_strtoint(trim_dict(basic, u'注册日期')) if trim_dict(basic, u'注册日期') is not None else None
                        reg_address = None
                        if u'住所' in basic.keys():
                            reg_address = trim_dict(basic, u'住所')
                        elif u'营业场所' in basic.keys():
                            reg_address = trim_dict(basic, u'营业场所')
                        elif u'经营场所' in basic.keys():
                            reg_address = trim_dict(basic, u'经营场所')
                        country = u"中国"
                        # city = u"上海市"
                        city = address_code_parsing(reg_address, reg_area_code, cnx)
                        oper_begin_date, oper_end_date = None, None
                        if u'经营期限自' in basic.keys():
                            oper_begin_date = date_strtoint(trim_dict(basic, u'经营期限自')) if trim_dict(basic, u'经营期限自') is not None else None
                        elif u'营业期限自' in basic.keys():
                            oper_begin_date = date_strtoint(trim_dict(basic, u'营业期限自')) if trim_dict(basic, u'营业期限自') is not None else None
                        if u'经营期限至' in basic.keys():
                            oper_end_date = date_strtoint(trim_dict(basic, u'经营期限至')) if trim_dict(basic, u'经营期限至') is not None else None
                        elif u'营业期限至' in basic.keys():
                            oper_end_date = date_strtoint(trim_dict(basic, u'营业期限至')) if trim_dict(basic, u'营业期限至') is not None else None
                        oper_scope = trim_dict(basic, u'经营范围')
                        reg_gov = basic[u'登记机关'].strip()  # NOT NULL
                        reg_gov_cd = reg_area_code  # NOT NULL
                        appr_date = date_strtoint(trim_dict(basic, u'核准日期')) if trim_dict(basic, u'核准日期') is not None else None
                        reg_status = trim_dict(basic, u'登记状态').split(u'（')[0] if trim_dict(basic, u'登记状态') is not None else None
                        reg_status_cd = None
                        if reg_status is not None:
                            reg_status_cd = db.sql_fetch_one(cnx, " select param_code from rongzidb.sys_param where value_name = %s ", [reg_status])[0]
                        # no = basic[u'注册号/']

                        sql = '''
                            INSERT INTO rongzidb.org_reg_info(org_reg_id, src_cd, org_id, org_name, org_type,
                            leg_reps, reg_cap, reg_cap_curr, found_date, reg_address,
                            country, city, oper_begin_date, oper_end_date, oper_scope,
                            reg_gov, reg_gov_cd, appr_date, reg_status,reg_status_cd ) VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                              '''

                        # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, org_name, org_type,
                        #                          leg_reps, reg_cap, reg_cap_curr, found_date, reg_address,
                        #                          country, city, oper_begin_date, oper_end_date, oper_scope,
                        #                          reg_gov, reg_gov_cd, appr_date, reg_status, reg_status_cd])

                        sql_groups.append(sql)
                        params_groups.append((org_reg_id, src_cd, org_id, org_name, org_type,
                                              leg_reps, reg_cap, reg_cap_curr, found_date, reg_address,
                                              country, city, oper_begin_date, oper_end_date, oper_scope,
                                              reg_gov, reg_gov_cd, appr_date, reg_status, reg_status_cd))
                        '''
                            <2> 企业信息变更
                        '''
                        if u'变更信息' in data[u'登记信息'].keys():
                            chginfo = data[u'登记信息'][u'变更信息']
                            sql = '''
                                INSERT INTO rongzidb.org_reg_chg(org_reg_id, src_cd,org_id, chg_proj, pre_chg, post_chg, chg_date)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            '''
                            for k in chginfo:
                                chg_proj = trim_dict(k, u'变更事项')
                                pre_chg = trim_dict(k, u'变更前内容')
                                post_chg = trim_dict(k, u'变更后内容')
                                temp = trim_dict(k, u'变更日期')
                                chg_date = date_strtoint(temp) if temp is not None else None
                                # cursor.execute(sql, (org_reg_id, src_cd, org_id, chg_proj, pre_chg, post_chg, chg_date))
                                # db.sql_insert(cnx,sql,[org_reg_id, src_cd, org_id, chg_proj, pre_chg, post_chg, chg_date])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, chg_proj, pre_chg, post_chg, chg_date))
                        '''

                            <3>企业股东及出资信息
                        '''

                        if u'股东信息' in data[u'登记信息'].keys():
                            eqs = data[u'登记信息'][u'股东信息']
                            sql = '''
                            INSERT INTO rongzidb.org_eq_aic
                            (org_id, org_reg_id, src_cd, holder_type, holder_name, cred_type, cred_num)
                            VALUES(%s, %s, %s, %s, %s, %s, %s)
                            '''
                            for eq in eqs:
                                holder_type = trim_dict(eq, u'股东类型')
                                holder_name = trim_dict(eq, u'股东')
                                cred_type = trim_dict(eq, u'证照/证件类型')
                                cred_num = trim_dict(eq, u'证照/证件号码')
                                # db.sql_insert(cnx, sql, [org_id, org_reg_id, src_cd, holder_type, holder_name, cred_type, cred_num])
                                sql_groups.append(sql)
                                params_groups.append((org_id, org_reg_id, src_cd, holder_type, holder_name, cred_type, cred_num))
                        '''
                            <4>企业主要人员
                        '''
                        if u'主要人员信息' in data[u'备案信息'].keys():
                            staffs = data[u'备案信息'][u'主要人员信息']
                            sql = '''
                            INSERT INTO rongzidb.org_staff(org_reg_id, src_cd,org_id, person_name, position, end_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            '''
                            for staff in staffs:
                                if (staff[u'姓名'] is None) or len(staff[u'姓名'].strip()) == 0:
                                    continue
                                person_name = staff[u'姓名'].strip()  # NOT NULL
                                position = trim_dict(staff, u'职务')
                                end_date = 19900101  # NOT NULL
                                # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, person_name, position, end_date])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, person_name, position, end_date))
                        '''
                            <5> 分支机构
                        '''
                        if u'分支机构信息' in data[u'备案信息'].keys():
                            branches = data[u'备案信息'][u'分支机构信息']
                            sql = '''
                            INSERT INTO rongzidb.org_branch(org_reg_id, src_cd, org_id, org_branch_name, org_branch_id, src_json)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            '''
                            duplicated_rmd_branches = []
                            names = []
                            for temp in branches:
                                if temp[u'名称'] in names:
                                    pass
                                else:
                                    names.append(temp[u'名称'])
                                    duplicated_rmd_branches.append(temp)
                            for branch in duplicated_rmd_branches:
                                org_branch_name = branch[u'名称'].strip()  # NOT NULL
                                sql_ = " select org_id from rongzidb.org_list where org_name = %s "
                                temp = db.sql_fetch_one(cnx, sql_, [org_branch_name])
                                org_branch_id, src_json = None, None
                                if temp is None:
                                    djjg = trim_dict(branch, u'登记机关')
                                    code = trim_dict(branch, u'注册号/统一社会信用代码')
                                    src_json = json.dumps({u'登记机关': djjg, u'注册号/统一社会信用代码': code})
                                else:
                                    org_branch_id = temp[0]
                                # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, org_branch_name, org_branch_id, src_json])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, org_branch_name, org_branch_id, src_json))
                        '''
                            <6>动产抵押登记信息
                        '''
                        if u'动产抵押登记信息' in data[u'动产抵押登记信息'].keys():
                            mortgage_mp = data[u'动产抵押登记信息'][u'动产抵押登记信息']
                            sql = '''
                            INSERT INTO rongzidb.org_mtg_mp(org_id, reg_num, reg_date, reg_gov, credit_vol, status)
                            VALUES (%s, %s, %s, %s, %s, %s )
                            '''
                            for mp in mortgage_mp:
                                reg_num = trim_dict(mp, u'登记编号')
                                reg_date = date_strtoint(trim_dict(mp, u'登记日期')) if trim_dict(mp, u'登记日期') is not None else None
                                reg_gov = trim_dict(mp, u'登记机关')
                                credit_vol = Decimal.from_float(float(trim_dict(mp, u'被担保债权数额').split(u'万')[0])) if trim_dict(mp,
                                                                                                                              u'被担保债权数额') is not None else None
                                status = status_strtoint(trim_dict(mp, u'状态')) if trim_dict(mp, u'状态') is not None else None
                                # db.sql_insert(cnx, sql, [org_id, reg_num, reg_date, reg_gov, credit_vol, status])
                                sql_groups.append(sql)
                                params_groups.append((org_id, reg_num, reg_date, reg_gov, credit_vol, status))
                        '''
                            <7>股权出质登记信息
                        '''
                        if u'股权出质登记信息' in data[u'股权出质登记信息'].keys():
                            mortgage_eq = data[u'股权出质登记信息'][u'股权出质登记信息']
                            sql = '''
                                INSERT INTO rongzidb.org_mtg_eq(org_id, reg_num, eq_name, eq_num, eq_cap, pn_name, pn_num, eq_date, status)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s )
                                    '''
                            for eq in mortgage_eq:
                                reg_num = trim_dict(eq, u'登记编号')
                                eq_name = trim_dict(eq, u'出质人')
                                eq_num = None
                                eq_cap = money_strtodecimal(trim_dict(eq, u'出质股权数额')) if trim_dict(eq, u'出质股权数额') is not None else None
                                pn_name = trim_dict(eq, u'质权人')
                                pn_num = eq[u'证照/证件号码']
                                eq_date = date_strtoint(trim_dict(eq, u'股权出质设立登记日期')) if trim_dict(eq, u'股权出质设立登记日期') is not None else None
                                status = status_strtoint(trim_dict(eq, u'状态')) if trim_dict(eq, u'状态') is not None else None
                                # db.sql_insert(cnx, sql, [org_id, reg_num, eq_name, eq_num, eq_cap, pn_name, pn_num, eq_date, status])
                                sql_groups.append(sql)
                                params_groups.append((org_id, reg_num, eq_name, eq_num, eq_cap, pn_name, pn_num, eq_date, status))

                        '''
                            <9> 经营异常
                        '''
                        if u'经营异常信息' in data[u'经营异常信息'].keys():
                            abns = data[u'经营异常信息'][u'经营异常信息']
                            sql = '''
                            INSERT INTO rongzidb.org_oper_abn
                            (org_reg_id, src_cd, org_id, into_reason, into_date, dec_gov_into, out_reason, out_date, dec_gov_out)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s )
                            '''
                            for abn in abns:
                                into_reason = trim_dict(abn, u'列入经营异常名录原因')
                                into_date = date_strtoint(trim_dict(abn, u'列入日期')) if trim_dict(abn, u'列入日期') is not None else None
                                dec_gov_into = trim_dict(abn, u'作出决定机关')
                                out_reason = trim_dict(abn, u'移出经营异常名录原因')
                                out_date = date_strtoint(trim_dict(abn, u'移出日期')) if trim_dict(abn, u'移出日期') is not None else None
                                dec_gov_out = trim_dict(abn, u'作出决定机关')
                                # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, into_reason, into_date, dec_gov_into, out_reason, out_date, dec_gov_out])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, into_reason, into_date, dec_gov_into, out_reason, out_date, dec_gov_out))
                        '''
                            <10> 严重违法
                        '''
                        if u'严重违法信息' in data[u'严重违法信息'].keys():
                            anti_legs = data[u'严重违法信息'][u'严重违法信息']
                            sql = '''
                                INSERT INTO rongzidb.org_ser_leg
                                (org_reg_id, src_cd, org_id, into_reason, into_date, dec_gov_into, out_reason, out_date, dec_gov_out)
                                VALUES
                                 '''
                            for anti in anti_legs:
                                into_reason = trim_dict(anti, u'列入严重违法企业名单原因')
                                into_date = date_strtoint(trim_dict(anti, u'列入日期')) if trim_dict(abn, u'列入日期') is not None else None
                                dec_gov_into = trim_dict(anti, u'作出决定机关')
                                out_reason = trim_dict(anti, u'移出严重违法企业名单原因')
                                out_date = date_strtoint(trim_dict(anti, u'移出日期')) if trim_dict(abn, u'移出日期') is not None else None
                                dec_gov_out = trim_dict(anti, u'作出决定机关')
                                db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, into_reason, into_date, dec_gov_into, out_reason, out_date, dec_gov_out])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, into_reason, into_date, dec_gov_into, out_reason, out_date, dec_gov_out))
                        '''
                            <11> 抽检检查
                        '''
                        if u'抽查检查信息' in data[u'抽查检查信息'].keys():
                            checks = data[u'抽查检查信息'][u'抽查检查信息']
                            sql = '''
                                INSERT INTO rongzidb.org_sam_chk
                                (org_reg_id, src_cd, org_id, chk_gov, chk_type, chk_date, chk_res)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                '''
                            for check in checks:
                                chk_gov = trim_dict(check, u'检查实施机关')
                                chk_type = trim_dict(check, u'类型')
                                chk_date = date_strtoint(trim_dict(check, u'日期')) if trim_dict(check, u'日期') is not None else None
                                chk_res = trim_dict(check, u'结果')
                                # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, chk_gov, chk_type, chk_date, chk_res])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, chk_gov, chk_type, chk_date, chk_res))
                        '''
                            <13> 行政处罚信息
                        '''
                        if u'行政处罚信息' in data[u'行政处罚信息'].keys():
                            punishes = data[u'行政处罚信息'][u'行政处罚信息']
                            sql = '''
                                INSERT INTO rongzidb.org_ad_sc
                                (org_reg_id, src_cd, org_id, sc_num, sc_type, sc_text, sc_gov, sc_date)
                                VALUES
                                '''
                            for punish in punishes:
                                sc_num = trim_dict(punish, u'行政处罚决定书文号')
                                sc_type = trim_dict(punish, u'违法行为类型')
                                sc_text = trim_dict(punish, u'行政处罚内容')
                                sc_gov = trim_dict(punish, u'作出行政处罚决定机关名称')
                                sc_date = date_strtoint(trim_dict(punish, u'作出行政处罚决定日期')) if trim_dict(punish, u'作出行政处罚决定日期') is not None else None
                                # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, sc_num, sc_type, sc_text, sc_gov, sc_date])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, sc_num, sc_type, sc_text, sc_gov, sc_date))
                    elif key == 'qygs':  # 企业公示
                        '''
                           <8>知识产权出质登记信息
                        '''
                        if u'知识产权出质登记信息' in data[u'知识产权出质登记信息'].keys():
                            mortgage_il = data[u'知识产权出质登记信息'][u'知识产权出质登记信息']
                            sql = '''INSERT INTO rongzidb.org_mtg_il
                            (org_id, org_name, il_style, il_name, pn_name, il_date_desc, status)
                            VALUES(%s, %s, %s, %s, %s, %s, %s)
                            '''
                            for il in mortgage_il:
                                il_style = trim_dict(il, u'种类')
                                il_name = trim_dict(il, u'出质人名称')
                                pn_name = trim_dict(il, u'质权人名称')
                                il_date_desc = trim_dict(il, u'质权登记期限') if trim_dict(il, u'质权登记期限') is not None else None
                                status = status_strtoint(trim_dict(il, u'状态')) if trim_dict(il, u'状态') is not None else None
                                # db.sql_insert(cnx, sql, [sql, org_id, org_name, il_style, il_name, pn_name, il_date, status])
                                sql_groups.append(sql)
                                params_groups.append((org_id, org_name, il_style, il_name, pn_name, il_date_desc, status))
                        '''
                           <12> 行政许可
                        '''
                        if u'行政许可信息' in data[u'行政许可信息'].keys():
                            grants = data[u'行政许可信息'][u'行政许可信息']
                            sql = '''
                            INSERT INTO rongzidb.org_ad_lc
                            (org_reg_id, src_cd, org_id, lc_num, lc_name, lc_begin_date, lc_end_date, lc_gov, lc_text, status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            '''
                            for grant in grants:
                                lc_num = trim_dict(grant, u'许可文件编号')
                                lc_name = trim_dict(grant, u'许可文件名称')
                                lc_begin_date = date_strtoint(trim_dict(grant, u'有效期自')) if trim_dict(grant, u'有效期自') is not None else None
                                lc_end_date = date_strtoint(trim_dict(grant, u'有效期至')) if trim_dict(grant, u'有效期至') is not None else None
                                lc_gov = trim_dict(grant, u'许可机关')
                                lc_text = trim_dict(grant, u'许可内容')
                                status = status_strtoint(trim_dict(grant, u'状态')) if trim_dict(grant, u'状态') is not None else None
                                # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, lc_num, lc_name, lc_begin_date, lc_end_date, lc_gov, lc_text, status])
                                sql_groups.append(sql)
                                params_groups.append((org_reg_id, src_cd, org_id, lc_num, lc_name, lc_begin_date, lc_end_date, lc_gov, lc_text, status))
                        '''
                            <3-1> 股东信息
                        '''
                        if data[u'股东及出资信息'] is not None:
                            if u'股东及出资信息（币种与注册资本一致）' in data[u'股东及出资信息'].keys():
                                gdinfo = data[u'股东及出资信息'][u'股东及出资信息（币种与注册资本一致）']
                                sql = '''
                                INSERT INTO rongzidb.org_eq_epi
                                (org_id, org_reg_id, src_cd, holder_name, total_subs_cap, total_paid_cap,
                                subs_cap, subs_style, subs_date, paid_cap, paid_style, paid_date)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                '''
                                for info in gdinfo:
                                    holder_name = trim_dict(info, u'股东')
                                    total_subs_cap = money_strtodecimal(trim_dict(info, u'认缴额（万元）'))
                                    total_paid_cap = money_strtodecimal(trim_dict(info, u'实缴额（万元）')) if trim_dict(info, u'实缴额（万元）') is not None else None
                                    subs_style = trim_dict(info[u'认缴明细'], u'认缴出资方式')
                                    subs_cap = money_strtodecimal(trim_dict(info[u'认缴明细'], u'认缴出资额（万元）')) if trim_dict(info[u'认缴明细'],
                                                                                                                       u'认缴出资额（万元）') is not None else None
                                    subs_date = date_strtoint(trim_dict(info[u'认缴明细'], u'认缴出资日期')) if trim_dict(info[u'认缴明细'], u'认缴出资日期') is not None else None
                                    paid_style = trim_dict(info[u'实缴明细'], u'实缴出资方式')
                                    paid_cap = money_strtodecimal(trim_dict(info[u'实缴明细'], u'实缴出资额（万元）')) if trim_dict(info[u'实缴明细'],
                                                                                                                       u'实缴出资额（万元）') is not None else None
                                    paid_date = date_strtoint(trim_dict(info[u'实缴明细'], u'实缴出资日期')) if trim_dict(info[u'实缴明细'], u'实缴出资日期') is not None else None
                                    db.sql_insert(cnx, sql,
                                                  [org_id, org_reg_id, src_cd, holder_name, total_subs_cap, total_paid_cap, subs_cap, subs_style, subs_date,
                                                   paid_cap,
                                                   paid_style, paid_date])
                                    sql_groups.append(sql)
                                    params_groups.append((
                                        org_id, org_reg_id, src_cd, holder_name, total_subs_cap, total_paid_cap, subs_cap, subs_style, subs_date, paid_cap,
                                        paid_style, paid_date))

                        '''
                        <3-2> 股东信息变更
                        '''
                        if u'股权变更信息' in data.keys():
                            if u'股权变更信息' in data[u'股权变更信息'].keys():
                                gdchgs = data[u'股权变更信息'][u'股权变更信息']
                                sql = '''
                                INSERT INTO rongzidb.org_eq_chg
                                (org_reg_id, src_cd, org_id, holder_name, pre_eq_rate, post_eq_rate, chg_date)
                                VALUES(%s, %s, %s, %s, %s, %s, %s)
                                '''
                                for chg in gdchgs:
                                    holder_name = trim_dict(chg, u'股东')
                                    pre_eq_rate = Decimal.from_float(float(trim_dict(chg, u'变更前股权比例').split('%')[0])) if trim_dict(chg,
                                                                                                                                   u'变更前股权比例') is not None else None
                                    post_eq_rate = Decimal.from_float(float(trim_dict(chg, u'变更后股权比例').split('%')[0])) if trim_dict(chg,
                                                                                                                                    u'变更后股权比例') is not None else None
                                    chg_date = date_strtoint(trim_dict(chg, u'股权变更日期')) if trim_dict(chg, u'股权变更日期') is not None else None
                                    # db.sql_insert(cnx, sql, [org_reg_id, src_cd, org_id, holder_name, pre_eq_rate, post_eq_rate, chg_date])
                                    sql_groups.append(sql)
                                    params_groups.append((org_reg_id, src_cd, org_id, holder_name, pre_eq_rate, post_eq_rate, chg_date))
                    elif key == 'bmgs':  # 其他部门公示
                        pass
                    elif key == 'sfgs':  # 司法公示
                        pass
                        # cnx.commit()

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


def date_strtoint(datestr):
    return int(datetime.datetime.strptime(datestr, u'%Y年%m月%d日').strftime(u'%Y%m%d'))


def status_strtoint(status):
    if status == u'无效':
        return 0
    elif status == u'有效':
        return 1


def money_strtodecimal(money_string):
    return Decimal.from_float(float(money_string.split(u'万元')[0]))


def trim_dict(dict_, key_):
    return dict_[key_].strip() if ((dict_[key_] is not None) and len(dict_[key_]) != 0) else None


def test():
    cnxpool = db.sql_connect("default.ini", "spider_con_config")
    cnx = cnxpool.get_connection()
    cursor = cnx.cursor()
    cursor.execute(" insert into transaction_test(name) values (%s)", ("a",))
    cnx.commit()
    print cnx.autocommit
    cnx.autocommit = True
    cursor = cnx.cursor()
    cursor.execute(" insert into transaction_test(name) values (%s)", ("b",))
    cnx.commit()
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    # test()
    main()
