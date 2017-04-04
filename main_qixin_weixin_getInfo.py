#coding=utf-8

import sys, logging.config, os, time
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../../")

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

    #return {'http':'121.33.226.167:3128'}

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
        r = s.post(target_url, headers=headers, proxies=proxies, timeout=15, verify=False)
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


#获得公司首页信息
def getHomePageInfo(proxies, eid):
    target_url = "http://wx.qixin007.com/company/" + str(eid) + ".html"
    headers = {
        "Host":"wx.qixin007.com",
        "Accept":"application/json, text/plain, */*",
        "Origin":"http://wx.qixin007.com",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Referer":"http://wx.qixin007.com/company/ac6a078e-48f2-46ab-8bbf-1d8c2c63c9e5.html",
        "Cookie":"aliyungf_tc=AQAAAFfyHEQB9QIAFnKm3ytM0AL0pXJg; sid=s%3AhWqG5wBQ7-PVzdcpcMLnEiQpdSSZM83f.k0nBBWu%2BzAvqF%2FgSK8UyN98tUpwB6KJYYJno%2FkJSmGU; Hm_lvt_020971628e81872ff6d45847a9a8f09c=1468981082; Hm_lpvt_020971628e81872ff6d45847a9a8f09c=1468984231",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
    }
    status = 0
    resp = getResponse(target_url, proxies, headers)
    logging.getLogger().info("Get homePage_info code: %d" % resp['code'])

    if resp['code'] != 0:
        status = -1
        return [], status

    return resp['content'].text, status

#获得工商基本信息
def getBasicInfo(proxies, eid):
    target_url = "http://wx.qixin007.com/company-info/" + str(eid) + ".html"
    headers = {
        "Host":"wx.qixin007.com",
        "Accept":"application/json, text/plain, */*",
        "Origin":"http://wx.qixin007.com",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Referer":"http://wx.qixin007.com/company-info/ac6a078e-48f2-46ab-8bbf-1d8c2c63c9e5.html",
        "Cookie":"aliyungf_tc=AQAAAFfyHEQB9QIAFnKm3ytM0AL0pXJg; sid=s%3AhWqG5wBQ7-PVzdcpcMLnEiQpdSSZM83f.k0nBBWu%2BzAvqF%2FgSK8UyN98tUpwB6KJYYJno%2FkJSmGU; Hm_lvt_020971628e81872ff6d45847a9a8f09c=1468981082; Hm_lpvt_020971628e81872ff6d45847a9a8f09c=1468984231",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
    }
    status = 0
    resp = getResponse(target_url, proxies, headers)
    logging.getLogger().info("Get basic_info code: %d" % resp['code'])

    if resp['code'] != 0:
        status = -1
        return [], status

    return resp['content'].text, status

#获得变更信息
def getChangeInfo(proxies, eid):
    target_url = "http://wx.qixin007.com/company-change/" + str(eid) + ".html"
    headers = {
        "Host":"wx.qixin007.com",
        "Accept":"application/json, text/plain, */*",
        "Origin":"http://wx.qixin007.com",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Referer":"http://wx.qixin007.com/company-change/ac6a078e-48f2-46ab-8bbf-1d8c2c63c9e5.html",
        "Cookie":"aliyungf_tc=AQAAAFfyHEQB9QIAFnKm3ytM0AL0pXJg; sid=s%3AhWqG5wBQ7-PVzdcpcMLnEiQpdSSZM83f.k0nBBWu%2BzAvqF%2FgSK8UyN98tUpwB6KJYYJno%2FkJSmGU; Hm_lvt_020971628e81872ff6d45847a9a8f09c=1468981082; Hm_lpvt_020971628e81872ff6d45847a9a8f09c=1468984231",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
    }
    status = 0
    resp = getResponse(target_url, proxies, headers)
    logging.getLogger().info("Get change_info code: %d" % resp['code'])

    if resp['code'] != 0:
        status = -1
        return [], status

    return resp['content'].text, status

def work():
    site = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'qixin_weixin_site'")
    while True:
        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'qixin_weixin_getinfo'")
        value = json.loads(common_config)
        start = value['start']
        limit = value['limit']
        stop = value['stop']
        if stop == "True":
            logging.getLogger().info("Start to sleep 5 second.")
            time.sleep(5)
            continue

        tasks = db.sql_fetch_rows("SELECT `id`, `eid`, `crawl_status`, `company_name` FROM `qixin_list` WHERE `id` >= %d AND `id` < %d" % (start, start + limit))
        max_id = -1
        if tasks != None:
            for task in tasks:
                logging.getLogger().info(">>>>>>start to crawler %d" % task[0])
                id = task[0]
                max_id = max(id, max_id)
                if task[2] == 7:
                    logging.getLogger().info("end to crawler %d" % task[0])
                    continue

                eid = task[1]
                company_name = task[3]
                crawl_status = task[2]

                if task[2] & 1 == 0:
                    #获取首页信息
                    proxies = getProxy(site)
                    (homeinfo, status_hp) = getHomePageInfo(proxies, eid)
                    logging.getLogger().info("HomePage: finished")
                    if status_hp == 0:
                        crawl_status |= 1

                '''
                sleep_sec = random.random() + 0.5;
                logging.getLogger().info("sleep seconds %.3f" % sleep_sec)
                time.sleep(sleep_sec)
                '''

                if task[2] & 2 == 0:
                    #获取工商信息
                    proxies = getProxy(site)
                    (basicinfo, status_b) = getBasicInfo(proxies, eid)
                    logging.getLogger().info("Basicinfo: finished")
                    if status_b == 0:
                        crawl_status |= 2

                '''
                sleep_sec = random.random() + 0.5;
                logging.getLogger().info("sleep seconds %.3f" % sleep_sec)
                time.sleep(sleep_sec)
                '''

                if task[2] & 4 == 0:
                    #获取变更记录信息
                    proxies = getProxy(site)
                    (changeinfo, status_c) = getChangeInfo(proxies, eid)
                    logging.getLogger().info("Changeinfo: finished")
                    if status_c == 0:
                        crawl_status |= 4

                '''
                sleep_sec = random.random() + 0.5;
                logging.getLogger().info("sleep seconds %.3f" % sleep_sec)
                time.sleep(sleep_sec)
                '''

                update = "UPDATE `qixin_list` SET `src_homepageinfo` = %s, `src_basicinfo` = %s, `src_changeinfo` = %s, `crawl_status` = %s WHERE id = %s"
                db.sql_update(update , [homeinfo, basicinfo, changeinfo, crawl_status, id])

                logging.getLogger().info("end to crawler %d" % task[0])
        else:
            logging.getLogger().info("No company could be find, start to sleep 10 second.")
            time.sleep(10)
            continue

        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'qixin_weixin_getinfo'")
        value = json.loads(common_config)
        stop = value['stop']
        common_config = '{"start" : %d, "limit" : %d, "stop" : "%s"}' % (max_id + 1, limit, stop)
        db.sql_update("UPDATE `common_config` SET `value` = '%s' WHERE `key` = 'qixin_weixin_getinfo'" % (common_config))
        #break

if __name__ == '__main__':
    logging.config.fileConfig("conf_log.conf")
    logging.getLogger().info("start main")
    logging.getLogger().info("start to init mysql connection pool")
    db.sql_connect("default.ini", "spider_con_config")
    work()
    logging.getLogger().info("end main exit")
