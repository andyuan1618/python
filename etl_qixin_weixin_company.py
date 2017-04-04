# coding=utf-8

import pack.sql_connect as sql_conn
import os
import logging
import logging.config
import json
import re

def fetch_rows(db, tb_name, starts):
    srcs = []
    stmt = "select `id`, `web_extra` from %s where `web_extra` is not null limit %d, 10000" % (tb_name, starts)
    for row in db.sql_fetch_rows(stmt):
        srcs.append([row[0], row[1]])
    return srcs

def main():
    #连接数据库
    db_con_config = {"host":"10.51.1.251", "user":"app_qa_r", "password":"SUw^(7^MH0", "database":"spiderdb_test", "port":3311, "charset":"utf8"}
    db_spider = sql_conn._MySQL(**db_con_config)
    db_spider.sql_connect()

    #执行main任务
    if not os.path.exists("d:/qixin_weixin_company"):
        os.makedirs("d:/qixin_weixin_company")

    #设置fetch界限
    starts = 0
    nums = 0
    tb_name = 'qixin_weixin_company'
    reg_illegal = re.compile('\d{0,4}-?\.{3}')
    reg_name = re.compile('[\s\-（）()]')
    while True:
        srcs = fetch_rows(db_spider, tb_name, starts)
        if nums % 10 == 0:
            f = open('d:/qixin_weixin_company/data_%d.txt' % int(nums / 10), 'a')
            f.write("ID  联系人  电话号码  公司名称\n")
            f.close()
        if len(srcs) == 0:
            break
        f = open('d:/qixin_weixin_company/data_%d.txt' % int(nums / 10), 'a')
        for row in srcs:
            dic = json.loads(row[1])
            if dic['phone'] is None or re.match(reg_illegal, dic['phone']) or dic['phone'].strip() == '':
                continue
            line = '@'.join([str(row[0]), re.sub(reg_name, '', dic['oper_name']), str(dic['phone'].strip()), dic['name'].strip()])
            try:
                f.write(line + '\n')
            except Exception as e:
                logging.getLogger().error(e)
                logging.getLogger().info("Error for id:%d & linkman:%s & tel:%s & company_name:%s" % (row[0], dic['oper_name'], dic['phone'], dic['name']))
        nums += 1
        starts += 10000
        f.close()

    db_spider.sql_close()


if __name__ == '__main__':
    logging.config.fileConfig('./config_log.txt')
    logging.getLogger().info("initial main task")
    main()
    logging.getLogger().info("end main task")
