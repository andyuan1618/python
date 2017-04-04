# coding=utf-8

import pack.sql_connect as sql_conn
import os
import logging
import logging.config
import json
import re

def fetch_rows(db, tb_name, starts):
    srcs = []
    stmt = "select `id`, `value` from %s where `value` is not null and `site` = 'qichacha_weixin' limit %d, 10000" % (tb_name, starts)
    for row in db.sql_fetch_rows(stmt):
        srcs.append([row[0], row[1]])
    return srcs

def main():
    #连接数据库
    db_con_config = {"host":"10.51.1.251", "user":"app_qa_r", "password":"SUw^(7^MH0", "database":"spiderdb_test", "port":3311, "charset":"utf8"}
    db_spider = sql_conn._MySQL(**db_con_config)
    db_spider.sql_connect()

    #执行main任务
    if not os.path.exists("d:/common_data_all"):
        os.makedirs("d:/common_data_all")

    #设置fetch界限
    starts = 0
    nums = 0
    tb_name = 'common_data_all'
    reg_tel = re.compile('\s+')
    reg_name = re.compile('[\s\-（）()]')
    while True:
        srcs = fetch_rows(db_spider, tb_name, starts)
        if nums % 10 == 0:
            f = open('d:/common_data_all/data_%d.txt' % int(nums / 10), 'a')
            f.write("ID  联系人  电话号码  公司名称\n")
            f.close()
        if len(srcs) == 0:
            break
        f = open('d:/common_data_all/data_%d.txt' % int(nums / 10), 'a')
        for row in srcs:
            dic = json.loads(row[1])
            if not 'data' in dic.keys() or dic['data'] is None:
                continue
            if not 'Company' in dic['data'].keys() or dic['data']['Company'] is None:
                continue
            if not 'PhoneNumber' in dic['data']['Company']['ContactInfo'].keys():
                continue
            if dic['data']['Company']['ContactInfo']['PhoneNumber'] is None or dic['data']['Company']['ContactInfo']['PhoneNumber'].strip() == '--' or dic['data']['Company']['ContactInfo']['PhoneNumber'].strip() == '':
                continue
            if not 'OperName' in dic['data']['Company'].keys() or dic['data']['Company']['OperName'] is None:
                dic['data']['Company']['OperName'] = '--'
            try:
                line = '@'.join([str(row[0]), re.sub(reg_name, '', dic['data']['Company']['OperName']), re.sub(reg_tel, '', str(dic['data']['Company']['ContactInfo']['PhoneNumber'])), dic['data']['Company']['Name'].strip()])
            except Exception as e:
                logging.getLogger().info("ErrorId: %d" % row[0])
            try:
                f.write(line + '\n')
            except Exception as e:
                logging.getLogger().error(e)
                logging.getLogger().info("Error for id:%d & linkman:%s & tel:%s & company_name:%s" % (row[0], dic['data']['Company']['OperName'], dic['data']['Company']['ContactInfo']['PhoneNumber'], dic['data']['Company']['Name']))
        nums += 1
        starts += 10000
        f.close()

    db_spider.sql_close()


if __name__ == '__main__':
    logging.config.fileConfig('./config_log.txt')
    logging.getLogger().info("initial main task")
    main()
    logging.getLogger().info("end main task")
