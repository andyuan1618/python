#coding=utf-8

import sys, logging.config
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../")

import requests, io, ConfigParser, json, random
import app.common.db as db
from requests.exceptions import ProxyError, ConnectTimeout, ReadTimeout
from random import Random
from urllib import quote

def random_str(randomlength = 8):
    str = ''
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        str += chars[random.randint(0, length)]
    return str


def getCookie():
    return random_str() + '-' + random_str(4) + '-' + random_str(4) + '-' + random_str(4) + '-' + random_str(12)


def getHeader():
    headers = {
        'Host': '120.52.121.75:8443',
        'Accept': '*/*',
        'Proxy-Connection': 'keep-alive',
        'Cookie': '',
        'User-Agent': 'Mozilla/5.0 (Ios;9.3;iPhone;iPhone);Version/2.2.0;ISN_GSXT',
        'Accept-Language': 'zh-Hans-CN;q=1',
    }
    headers['Cookie'] = getCookie()
    return headers


def getProxy(site):
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("default.ini", encoding="utf8"))
    host = json.loads(config.get("default", "proxies_config"))

    proxies = {
        'http': '',
    }

    url = 'http://%s/getproxy?site=%s&' % (host['url'], site)

    try:
        logging.getLogger().info("start to get proxyip")
        s = requests.Session()
        r = s.get(url)
        proxies['http'] = json.loads(r.text)['http_proxy']
        logging.getLogger().info("suc got proxyip: %s" % str(proxies['http']))
    except Exception, e:
        logging.getLogger().error("failed get proxyip")
        logging.getLogger().exception(e)

    return proxies

def getResponse(target_url, proxies, headers={}):
    s = requests.Session()

    # load config file
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("default.ini", encoding="utf8"))
    adapter_config = json.loads(config.get("default", "adapter_config"))

    adapter = requests.adapters.HTTPAdapter(**adapter_config)
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    try:
        r = s.get(target_url, headers=headers, proxies=proxies, verify=False, timeout=15)
    except ConnectTimeout, e:
        logging.getLogger().error("connect to %s timeout" % target_url)
        logging.getLogger().info("proxies: %s" % proxies)
        logging.getLogger().exception(e)
        return {'code': -1, 'content': u'connect time out'}
    except ReadTimeout, e:
        logging.getLogger().error("read %s timeout" % target_url)
        logging.getLogger().info("proxies: %s" % proxies)
        logging.getLogger().exception(e)
        return {'code': -2, 'content': u'read time out'}
    except ProxyError, e:
        logging.getLogger().error("proxy：%s error" % (proxies))
        logging.getLogger().info("proxies: %s" % proxies)
        logging.getLogger().exception(e)
        return {'code': -3, 'content': u'proxy error'}

    return {'code' : 0, 'content' : r.text}

def getCompanyList(compname, areacode, headers, proxies):
    compname = quote(compname)
    url = 'https://120.52.121.75:8443/QuerySummary?AreaCode=%s&Limit=50&Page=1&Q=%s' % (areacode, compname)
    print url
    print headers
    print proxies
    res = getResponse(url, headers, proxies)
    if (0 == res['code']):  # success
        return json.loads(res['content'])['RESULT']
    elif (res['code'] == -1 or res['code'] == -2 or res['code'] == -3):  # page is missed
        pass


def getCompanyInfo(complist, headers, proxies):
    results = []
    try:
        for compinfo in complist:
            regno = compinfo['REGNO']
            id = compinfo['ID']
            entname = compinfo['ENTNAME']
            areacode = compinfo['AREACODE']
            querystr = quote(entname)
            url = 'https://120.52.121.75:8443/QueryGSInfo?AreaCode=%s&EntId=%s&EntNo=%s&Info=All&Limit=50&Page=1&Q=%s' % (areacode, id, regno, querystr)
            res = getResponse(url, headers, proxies)
            results.append(res)
    except ValueError, e:
        pass
    return results


def work():
    proxies = {
        'https': 'http://rongzi:rongzi@121.239.232.94:8888'
    }
    headers = getHeader()
    print headers
    complist = getCompanyList('民众信息部', 420000, headers, proxies)
    print complist
    info = getCompanyInfo(complist, headers, proxies)
    return info

if __name__ == '__main__':
    #logging.config.fileConfig("conf_log.conf")
    #logging.getLogger().info("start main" )
    #logging.getLogger().info("start to init mysql connection pool" )
    #db.sql_connect("default.ini", "spider_con_config")
    #work()
    #logging.getLogger().info("end main exit" )
    #print getCookie()
    print work()
