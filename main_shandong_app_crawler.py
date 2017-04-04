#coding=utf-8

import sys, logging.config, os, time
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../")

import requests, io, ConfigParser, json
import app.common.db as db
from requests.exceptions import ProxyError, ConnectTimeout, ReadTimeout
from urllib import quote
import random


#获得代理IP
def getProxy(site):
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("default.ini", encoding="utf8"))
    host = json.loads(config.get("default", "proxies_config"))

    proxies = {
        'http': '',
    }
    #return {'http':'222.211.65.72:8080'}

    url = 'http://%s/getproxy?site=%s&' % (host['url'], site)

    try:
        logging.getLogger().info("start to get proxyip")
        s = requests.Session()
        r = s.get(url)
        proxies['http'] = json.loads(r.text)['http_proxy']
        logging.getLogger().info("suc got proxyip: %s" % str(proxies['http']))
    except Exception as e:
        logging.getLogger().error("failed get proxyip")
        logging.getLogger().exception(e)

    return proxies


#发送一次网络请求
def getResponse(target_url, proxies, headers):
    s = requests.Session()

    # load config file
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("default.ini", encoding="utf8"))
    adapter_config = json.loads(config.get("default", "adapter_config"))

    adapter = requests.adapters.HTTPAdapter(**adapter_config)
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    try:
        r = s.get(target_url, headers=headers, proxies=proxies, timeout=15, verify=False)
        logging.getLogger().info(r.text)
    except ConnectTimeout as e:
        logging.getLogger().error("connect to %s timeout" % target_url)
        logging.getLogger().info("proxies: %s" % proxies)
        logging.getLogger().exception(e)
        return {'code': -1, 'content': u'connect time out'}
    except ReadTimeout as e:
        logging.getLogger().error("read %s timeout" % target_url)
        logging.getLogger().info("proxies: %s" % proxies)
        logging.getLogger().exception(e)
        return {'code': -2, 'content': u'read time out'}
    except ProxyError as e:
        logging.getLogger().error("proxy：%s error" % (proxies))
        logging.getLogger().info("proxies: %s" % proxies)
        logging.getLogger().exception(e)
        return {'code': -3, 'content': u'proxy error'}

    return {'code' : 0, 'content' : r}


#获得查询列表
def getSearchList(proxies, company_name):
    target_url = 'http://mapp.gdgs.gov.cn:8448/publicity/v1.0/QuerySummary?Q=' + quote(company_name.encode('utf-8')) + '&Page=1&Limit=50'
    headers = {
        "Host":"mapp.gdgs.gov.cn:8448",
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Content-Type":"application/json; charset=utf-8",
        "Origin":"http://mapp.gdgs.gov.cn:8449",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Referer":"http://mapp.gdgs.gov.cn:8449/publicity/search.html?gdgsAccessKey=d2VpeGluMDAwMTIwMjYwMTE1NDkzMTI1OTU=",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
    }
    resp = getResponse(target_url=target_url, proxies=proxies, headers=headers)

    if resp['code'] != 0:
        return [], resp['code']
    #print  resp['content'].text
    #print  resp['code']

    return resp['content'].text, resp['code']

#解析查询列表
def parseSearchList(json_str):
    #json_str = '{"errcode":"0","result":[{"GSDJLX":"1","entName":"梅州好玩信息科技有限公司","entType":"有限责任公司","estDate":"2015年01月20日","id":"007b94fe-014b-1000-e003-d0040a090115","leRep":"倪广雄","regNo":"441421000042080","regOrg":"梅州市工商行政管理局梅县分局"},{"GSDJLX":"1","entName":"广东金赋信息科技有限公司","entType":"有限责任公司","estDate":"2002年04月28日","id":"0055ab8b-f17e-4769-a3f0-d6c0d67a63cd","leRep":"任泳谊","regNo":"440600000006325","regOrg":"佛山市工商行政管理局"},{"GSDJLX":"1","entName":"东莞市银景信息科技有限公司","entType":"有限责任公司","estDate":"2016年07月19日","id":"006609b6-0156-1000-e001-193b0a0c0115","leRep":"曾庆栋","regNo":"441900003249735","regOrg":"东莞市工商行政管理局"},{"GSDJLX":"1","entName":"东莞市洛帆信息科技有限公司","entType":"有限责任公司","estDate":"2016年07月19日","id":"006609b6-0156-1000-e001-17340a0c0115","leRep":"李振帆","regNo":"441900003249671","regOrg":"东莞市工商行政管理局"},{"GSDJLX":"1","entName":"东莞市寰荣信息科技有限公司","entType":"有限责任公司","estDate":"2015年08月06日","id":"0051c1a1-014f-1000-e000-2e9f0a76b50b","leRep":"易法姣","regNo":"441900002606319","regOrg":"东莞市工商行政管理局"},{"GSDJLX":"1","entName":"佛山市壮游信息科技有限公司","entType":"有限责任公司","estDate":"2013年09月10日","id":"001396f0-0141-1000-e000-7bec0a060116","leRep":"伍海辉","regNo":"440602000313625","regOrg":"佛山市禅城区工商行政管理局"},{"GSDJLX":"1","entName":"佛山市众微信息科技有限公司","entType":"有限责任公司","estDate":"2015年03月12日","id":"001f68be-014c-1000-e007-4b820a060116","leRep":"潘正洋","regNo":"440602000401496","regOrg":"佛山市禅城区工商行政管理局"},{"GSDJLX":"1","entName":"佛山翼家居信息科技有限公司","entType":"有限责任公司","estDate":"2015年03月13日","id":"001f68be-014c-1000-e009-ba2f0a060116","leRep":"黄厚荣","regNo":"440602000401742","regOrg":"佛山市顺德区市场监督管理局"},{"GSDJLX":"1","entName":"江门市平安信息科技有限公司","entType":"有限责任公司","estDate":"2008年05月19日","id":"00378d7b-011a-1000-e000-d2930a0e0115","leRep":"古永强","regNo":"440703000012812","regOrg":"江门市蓬江区市场监督管理局"},{"GSDJLX":"1","entName":"中山市易海购信息科技有限公司","entType":"有限责任公司","estDate":"2015年06月17日","id":"004cf1ce-014e-1000-e000-2a180a0d0114","leRep":"陈平","regNo":"442000001192485","regOrg":"中山市工商行政管理局"}]}'
    try:
        content_str = json.loads(json_str)
        if int(content_str['errcode']) != 0:
            logging.getLogger().error('请求异常')
            return {'code':-1, 'content':'请求异常'}
    except Exception as e:
        logging.getLogger().error('数据解析失败')
        return {'code':-2, 'content':'数据解析异常'}
    total_num = len(content_str['result'])
    start = 0
    items = content_str['result']
    results = []
    ent_no = None
    while total_num > start:
        temp_res = []
        temp_compinfo = items[start]
        company_name = temp_compinfo['entName']
        ent_id = temp_compinfo['id']
        ent_no = temp_compinfo['regNo']
        temp_res.append(company_name)
        temp_res.append(ent_id)
        temp_res.append(ent_no)
        temp_res.append(str(temp_compinfo))
        results.append(temp_res)
        start += 1
    return results, ent_no

#存列表信息
def storeSearchList(data, id, status):
    #print datas
    if len(data) == 0:
        if status == 0:
            db.sql_update("UPDATE `common_company_name` SET `guangdong_weixin_status` = 2 WHERE `id` = %d" % id)
        else:
            db.sql_update("UPDATE `common_company_name` SET `guangdong_weixin_status` = 3 WHERE `id` = %d" % id)
        return
    else:
        db.sql_update("UPDATE `common_company_name` SET `guangdong_weixin_status` = 1 WHERE `id` = %d" % id)
    insert_stmt = "INSERT INTO `guangdong_weixin_list`(company_name, ent_id, ent_no, web_extra, crawl_time) VALUES(%s, %s, %s, %s, NOW())"
    insert_data = []
    for item in data:
        insert_temp = []
        company_name = item[0]
        ent_id = item[1]
        ent_no = item[2]
        web_extra = item[3]
        exist_code = db.sql_fetch_one("SELECT * FROM `guangdong_weixin_list` WHERE `ent_no` = '%s'" % ent_no)
        if exist_code == None:
            insert_temp.append(company_name)
            insert_temp.append(ent_id)
            insert_temp.append(ent_no)
            insert_temp.append(json.dumps(eval(web_extra), encoding='utf-8', ensure_ascii=False))
            insert_data.append(insert_temp)
    try:
        db.sql_insert_many(insert_stmt, insert_data)
    except Exception as e:
        logging.error('error to insert into `guangdong_weixin_list`')
        logging.exception(e)

#获得工商基本信息
def getBasicInfo(proxies, ent_id, ent_no):
    target_url = 'http://mapp.gdgs.gov.cn:8448/publicity/v1.0/QueryGSInfo?EntId=' + ent_id + '&EntNo=' + ent_no + '&Info=All'
    headers = {
        "Host":"mapp.gdgs.gov.cn:8448",
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Content-Type":"application/json; charset=utf-8",
        "Origin":"http://mapp.gdgs.gov.cn:8449",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Referer":"http://mapp.gdgs.gov.cn:8449/publicity/search.html?gdgsAccessKey=d2VpeGluMDAwMTIwMjYwMTE1NDkzMTI1OTU=",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
    }
    resp = getResponse(target_url=target_url, proxies=proxies, headers=headers)

    if resp['code'] != 0:
        return [], resp['code']
    #print  resp['content'].text
    #print  resp['code']
    return resp['content'].text, resp['code']

#获得企业公示
def getQyInfo(proxies, ent_id, ent_no):
    target_url = 'http://mapp.gdgs.gov.cn:8448/publicity/v1.0/QueryQY?EntId=' + ent_id + '&EntNo=' + ent_no + '&Info=All'
    headers = {
        "Host":"mapp.gdgs.gov.cn:8448",
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Content-Type":"application/json; charset=utf-8",
        "Origin":"http://mapp.gdgs.gov.cn:8449",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Referer":"http://mapp.gdgs.gov.cn:8449/publicity/search.html?gdgsAccessKey=d2VpeGluMDAwMTIwMjYwMTE1NDkzMTI1OTU=",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
    }
    resp = getResponse(target_url=target_url, proxies=proxies, headers=headers)

    if resp['code'] != 0:
        return [], resp['code']
    #print  resp['content'].text
    #print  resp['code']
    return resp['content'].text, resp['code']

#获得其他信息
def getOthInfo(proxies, ent_id, ent_no):
    target_url = 'http://mapp.gdgs.gov.cn:8448/publicity/v1.0/QueryOth?EntId=' + ent_id + '&EntNo=' + ent_no + '&Info=All'
    headers = {
        "Host":"mapp.gdgs.gov.cn:8448",
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Content-Type":"application/json; charset=utf-8",
        "Origin":"http://mapp.gdgs.gov.cn:8449",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Referer":"http://mapp.gdgs.gov.cn:8449/publicity/search.html?gdgsAccessKey=d2VpeGluMDAwMTIwMjYwMTE1NDkzMTI1OTU=",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
    }
    resp = getResponse(target_url=target_url, proxies=proxies, headers=headers)

    if resp['code'] != 0:
        return [], resp['code']
    #print  resp['content'].text
    #print  resp['code']
    return resp['content'].text, resp['code']

#获得司法协助
def getJudInfo(proxies, ent_id, ent_no):
    target_url = 'http://mapp.gdgs.gov.cn:8448/publicity/v1.0/QueryJud?EntId=' + ent_id + '&EntNo=' + ent_no + '&Info=All'
    headers = {
        "Host":"mapp.gdgs.gov.cn:8448",
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Content-Type":"application/json; charset=utf-8",
        "Origin":"http://mapp.gdgs.gov.cn:8449",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Referer":"http://mapp.gdgs.gov.cn:8449/publicity/search.html?gdgsAccessKey=d2VpeGluMDAwMTIwMjYwMTE1NDkzMTI1OTU=",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
    }
    resp = getResponse(target_url=target_url, proxies=proxies, headers=headers)

    if resp['code'] != 0:
        return [], resp['code']
    #print  resp['content'].text
    #print  resp['code']
    return resp['content'].text, resp['code']

#获取公司具体信息
def getInfo(proxies, ent_no):
    if ent_no != None:
        company = db.sql_fetch_one("SELECT `id`, `ent_id`, `ent_no`, `crawl_status` FROM `guangdong_weixin_list` WHERE `ent_no` = '%s'" % (ent_no))
        id = company[0]
        ent_id = company[1]
        crawl_status = company[3]
        gs_info = None
        qy_info = None
        qt_info = None
        sf_info = None

        #工商公示
        logging.getLogger().info("BasicInfo")
        if crawl_status & 1 == 0:
            (gs_info, status_gs) = getBasicInfo(proxies, ent_id, ent_no)
            logging.getLogger().info(gs_info)
            logging.getLogger().info("BasicInfo: finished")
            if status_gs == 0:
                crawl_status |= 1

        logging.getLogger().info("QyInfo")
        #企业公示
        if crawl_status & 2 == 0:
            (qy_info, status_qy) = getQyInfo(proxies, ent_id, ent_no)
            logging.getLogger().info(qy_info)
            logging.getLogger().info("QyInfo: finished")
            if status_qy == 0:
                crawl_status |= 2

        logging.getLogger().info("OthInfo")
        #其他部门
        if crawl_status & 4 == 0:
            (qt_info, status_qt) = getOthInfo(proxies, ent_id, ent_no)
            logging.getLogger().info(qt_info)
            logging.getLogger().info("OthInfo: finished")
            if status_qt == 0:
                crawl_status |= 4

        logging.getLogger().info("JudInfo")
        #司法协助
        if crawl_status & 8 == 0:
            (sf_info, status_sf) = getJudInfo(proxies, ent_id, ent_no)
            logging.getLogger().info(sf_info)
            logging.getLogger().info("JudInfo: finished")
            if status_sf == 0:
                crawl_status |= 8

        update = "UPDATE `guangdong_weixin_list` set crawl_status = %d, src_gongshang = '%s', `src_qiye` = '%s'," \
                 "`src_other` = '%s', `src_judicial` = '%s'WHERE `id` = %d"
        db.sql_update(update % (crawl_status, gs_info, qy_info, qt_info, sf_info, id))

def work():
    site = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'guangdong_weixin_site'")
    while True:
        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'guangdong_weixin_crawler'")
        try:
            value = json.loads(common_config)
        except Exception as e:
            logging.getLogger().info("common_config parse error")
            logging.getLogger().error(e)

        start = value['start']
        limit = value['limit']
        stop = value['stop']
        if stop == "True":
            logging.getLogger().info("Start to sleep 5 second.")
            time.sleep(5)
            continue

        tasks = db.sql_fetch_rows("SELECT `id`, `name`, `guangdong_weixin_status` FROM `common_company_name` WHERE `id` >= %d AND `area_code` = 440000 LIMIT %d "% (start, limit))
        if tasks != None:
            for task in tasks:
                logging.getLogger().info(">>>>>>>>>>>start to crawler %d" % task[0])
                if task[2] != 0:
                    logging.getLogger().info("End to crawler %d" % task[0])
                    continue

                id = task[0]
                company_name = task[1]
                proxies = getProxy(site)
                data = []
                ent_id = None
                ent_no = None
                logging.getLogger().info(company_name)
                logging.getLogger().info("start to get the list")
                (html, status) = getSearchList(proxies, company_name)
                logging.getLogger().info(html)
                if status == 0:
                    logging.getLogger().info("start to parse the list")
                    (data, ent_no) = parseSearchList(html)
                    logging.getLogger().info("ent to parse the list")
                logging.getLogger().info("start to store the list")
                storeSearchList(data, id, status)
                logging.getLogger().info("end to store the list %d" % task[0])

                logging.getLogger().info("start to crawl the info %d" % task[0])
                #获取具体信息
                getInfo(proxies, ent_no)
                logging.getLogger().info("end to crawl the info %d" % task[0])

                '''
                sleep_sec = random.random() + 5;
                logging.getLogger().info("sleep seconds %.3f" % sleep_sec)
                time.sleep(sleep_sec)
                '''
        else:
            logging.getLogger().info("No keyword could be found, start to sleep 10 second.")
            time.sleep(10)
            continue

        if len(tasks) < limit:
            logging.getLogger().info("All keyword have be found, start to sleep 10 second.")
            time.sleep(10)

        start = max(tasks)[0] + 1
        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'guangdong_weixin_crawler'")
        value = json.loads(common_config)
        stop = value['stop']
        common_config = '{"start" : %d, "limit" : %d, "stop" : "%s"}' % (start, limit, stop)
        db.sql_update("UPDATE `common_config` SET `value` = '%s' WHERE `key` = 'guangdong_weixin_crawler'" % (common_config))
        #break

if __name__ == '__main__':
    logging.config.fileConfig("conf_log.conf")
    logging.getLogger().info("start main")
    logging.getLogger().info("start to init mysql connection pool")
    db.sql_connect("default.ini", "spider_con_config")
    try:
        work()
    except Exception as e:
        logging.getLogger().error("exit inormal")
        logging.getLogger().exception(e)
