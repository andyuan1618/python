#coding=utf-8

import sys, io, os
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../")

import requests, logging.config, ConfigParser, json, re, hashlib, time, random
import app.common.db as db
from requests.exceptions import ProxyError, ConnectTimeout, ReadTimeout, ConnectionError
from urllib import quote
from lxml import etree


#文本清洗
def getText(value):
    if value == None or len(value) == 0 or value[0].text == None:
        return ''
    else:
        p = re.compile("\s+")
        #去掉字符两边对于的空格和其它字符
        tmp =  re.sub(p, '', value[0].text)
        #去掉字符中间空格
        return re.sub(ur"[  　　 　 　]+", "", tmp)


#获得代理IP
def getProxy(site):
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("default.ini", encoding="utf8"))
    host = json.loads(config.get("default", "proxies_config"))

    proxies = {
        'http': '',
    }

    return {"http" : "http://rongzi:rongzi@49.73.122.144:8888"}

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
    return {'http':'http://rongzi:rongzi@117.82.49.170:8888',}
    return proxies


#发送一次网络请求
def getResponse(target_url, proxies, headers, allow_redirects):
    s = requests.Session()

    # load config file
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("default.ini", encoding="utf8"))
    adapter_config = json.loads(config.get("default", "adapter_config"))

    adapter = requests.adapters.HTTPAdapter(**adapter_config)
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    try:
        r = s.get(target_url, timeout=15, allow_redirects=allow_redirects, verify=False, headers=headers, proxies=proxies)
        #r = None
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
    except ConnectionError, e:
        logging.getLogger().error("Connection Error")
        logging.getLogger().exception(e)
        return {'code': -4, 'content': u'connection error'}

    return {'code' : 0, 'content' : r}

#获得Cookie的JSESSIONID
def getJsessionid(proxies):
    target_url = 'http://wx.saic.gov.cn/WebTools/saic/index?oid=gh_a35f9c0bf7d0'
    headers = {
        'Host':'wx.saic.gov.cn',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat',
        'Accept-Language':'zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4',
        'Accept-Encoding':'gzip, deflate',
    }

    #resp = getResponse(target_url=target_url, proxies, headers=headers, allow_redirects=False)
    #split the JSESSIONID out
    return "JSESSIONID=56A342F43B5474961148F7D1C66DDE96"
    #return resp['content'].headers['Set-Cookie'].split(";")[0]


#获取查询关键字
def getSearchKey(start, limit):
    res = db.sql_fetch_rows("SELECT `id`, `area_code`, `name` FROM `common_company_name` WHERE `id` > %d AND `id` < %d AND `gongshang_weixin_status` = 0" % (start, start + limit))
    return res


#获取省份名称和代码hash表
def getProvinceMap():
    res = db.sql_fetch_rows("SELECT `code`, `name` FROM common_area_code WHERE `type` = 2 order by `code`")
    map = {}
    for item in res:
        areacode = str(item[0])
        map[areacode] = item[1]
    return map


#获得省份代码对应的省份名称
def getProvince(province_map, areacode):
    return province_map[str(areacode)]

#模拟部分请求，伪装成爬虫
def getIndexPage(proxies, cookie):
    target_url = "http://wx.saic.gov.cn/WebTools/saic/index?code=011zd4K61K6gwX1cOLH610i0K61zd4Kz&state=ff6eac747e35438eb6aac27074285b57"
    headers = {
        "Host":"wx.saic.gov.cn",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Cookie":"",
    }
    headers['Cookie'] = cookie
    resp = getResponse(target_url, proxies, headers, True)

    target_url = "http://wx.saic.gov.cn/WebTools/saic/index?code=031JBqwb1Gxmsq0UOFyb1sBvwb1JBqwR&state=ff6eac747e35438eb6aac27074285b57"
    headers = {
        "Host":"wx.saic.gov.cn",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Cookie":"",
    }
    headers['Cookie'] = cookie
    resp = getResponse(target_url, proxies, headers, True)

    target_url = "http://wx.saic.gov.cn/WebTools/getJsSignConfig?pageUrl=http%3A//wx.saic.gov.cn/WebTools/saic/index%3Fcode%3D011zd4K61K6gwX1cOLH610i0K61zd4Kz%26state%3Dff6eac747e35438eb6aac27074285b57"
    headers = {
        "Host":"wx.saic.gov.cn",
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "X-Requested-With":"XMLHttpRequest",
        "Referer":"http://wx.saic.gov.cn/WebTools/saic/index?code=011zd4K61K6gwX1cOLH610i0K61zd4Kz&state=ff6eac747e35438eb6aac27074285b57",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Cookie":"",
    }
    headers['Cookie'] = cookie
    resp = getResponse(target_url, proxies, headers, True)


#获得查询列表
def getSearchList(proxies, cookie, company_name, area_code, area_name):
    print area_name
    target_url = "http://wx.saic.gov.cn/WebTools/saic/list/" + str(area_code) + "?from=country&p=" + quote(area_name.encode('utf-8')) + "&q=" + quote(company_name.encode('utf-8'))
    headers = {
        "Host":"wx.saic.gov.cn",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Referer":"",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Cookie":"",
    }
    headers['Referer'] = "http://wx.saic.gov.cn/WebTools/saic/search/" + str(area_code) + "?p=" + quote(area_name.encode('utf-8'))
    headers['Cookie'] = cookie
    resp = getResponse(target_url, proxies, headers, False)

    return resp['content'].text, target_url


#解析查询列表
def parseSearchList(html):
    datas = []
    root = etree.HTML(html)
    content = root.xpath(r"//div[@class='box1']//div[@class='contentbox']")
    for item in content:
        json_str = {}
        temp_data = []
        data_id = item.xpath(r"./@data-id")[0]
        data_provinceid = item.xpath(r"./@data-provinceid")[0]
        data_regno = item.xpath(r"./@data-regno")[0]
        data_entname = item.xpath(r"./@data-entname")[0]
        data_entcode = item.xpath(r"./@data-entcode")[0]
        json_str['data_provinceid'] = data_provinceid
        json_str['data_regno'] = data_regno
        json_str['data_id'] = data_id
        json_str['data_entname'] = data_entname
        json_str['data_entcode'] = data_entcode
        md5 = hashlib.md5()
        md5.update(str(json_str))
        hashcode = md5.hexdigest()
        temp_data.append(hashcode)
        temp_data.append(json_str)
        datas.append(temp_data)

    return datas


def storeSearchResult(datas, id):
    if len(datas) == 0:
        db.sql_update("UPDATE `common_company_name` SET `gongshang_weixin_status` = 2 WHERE `id` = %d" % id)
    else:
        db.sql_update("UPDATE `common_company_name` SET `gongshang_weixin_status` = 1 WHERE `id` = %d" % id)

    insert_data = []
    for item in datas:
        insert_temp = []
        hashcode = item[0]
        json_str = item[1]
        exist_code = db.sql_fetch_one("SELECT * FROM `gongshang_list` WHERE `webid_md5` = '%s'" % hashcode)
        if exist_code == None:
            insert_temp.append(str(json_str['data_entname']))
            insert_temp.append(str(hashcode))
            insert_temp.append(json.dumps(json_str, encoding='utf-8', ensure_ascii=False))
            insert_data.append(insert_temp)

    try:
        insert_stmt = 'INSERT INTO `gongshang_list`(company_name, webid_md5, web_extra) VALUES(%s, %s, %s)'
        db.sql_insert_many(insert_stmt, insert_data)
    except Exception, e:
        logging.error('error to insert into `gongshang_list`')
        logging.exception(e)


def getGSBasicInfo(referer, cookie, proxies, province_map):
    company_info = db.sql_fetch_one("SELECT * FROM `gongshang_list` WHERE `crawl_status` = 0 LIMIT 1")
    if company_info == None:
        return None
    id = company_info[0]
    webid_extra = json.loads(company_info[3])
    data_entname = webid_extra['data_entname']
    data_entcode = webid_extra['data_entcode']
    data_id = webid_extra['data_id']
    data_regno = webid_extra['data_regno']
    data_provinceid = webid_extra['data_provinceid']
    province_name = getProvince(province_map, data_provinceid)
    target_url = "http://wx.saic.gov.cn/WebTools/saic/content?Info=All&p=" + quote(province_name.encode('utf-8')) + "&ProvinceId=" + str(data_provinceid) + "&RegNo=" + str(data_regno) + "&EntId=" + str(data_id) + "&EntName=" + quote(data_entname.encode("utf-8")) + "&EntCode=" + str(data_entcode)
    headers = {
        "Host":"wx.saic.gov.cn",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Cookie":"",
        "Referer":"",
    }
    headers['Cookie'] = cookie
    headers['Referer'] = referer

    resp = getResponse(target_url, proxies, headers, allow_redirects=True)
    src = resp['content'].text

    if src.find(u"系统异常") != -1:
        logging.getLogger().info("系统异常")
        logging.getLogger().info(src.find(u"系统异常"))

    db.sql_update("UPDATE `gongshang_list` SET `src_basicinfo` = %s, `crawl_status` = 1, `crawl_time` = NOW() WHERE `id` = %s", (src, id))


def work():
    site = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'gongshang_weixin_site'")
    province_map = getProvinceMap()
    while True:
        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'gongshang_weixin_crawler'")
        value = json.loads(common_config)
        start = value['start']
        limit = value['limit']
        stop = value['stop']
        tasks = getSearchKey(start, limit)
        if stop == "True":
            break
        if tasks != None:
            for task in tasks:
                logging.getLogger().info("start to crawler %d" % task[0])
                id = task[0]
                areacode = task[1]
                company_name = task[2]
                proxies = getProxy(site)
                cookie = getJsessionid(proxies)
                #getIndexPage(proxies, cookie=cookie)
                province_name = getProvince(province_map, areacode)
                print province_name, areacode, company_name, proxies
                html, referer = getSearchList(proxies, cookie, company_name, areacode, province_name)
                datas = parseSearchList(html)
                print datas
                if html.find(u"系统异常") != -1:
                    logging.getLogger().info("系统异常")
                    logging.getLogger().info(html.find(u"系统异常"))
                storeSearchResult(datas, id)
                logging.getLogger().info("end to crawler %d" % task[0])
                getGSBasicInfo(referer, cookie, proxies, province_map)

                sleep_time = random.randint(4, 9)
                time.sleep(sleep_time)
                #os._exit(1)


        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'gongshang_weixin_crawler'")
        value = json.loads(common_config)
        stop = value['stop']
        if tasks == None:
            common_config = '{"start" : %d, "limit" : %d, "stop" : "%s"}' % (start + limit, limit, stop)
        else:
            common_config = '{"start" : %d, "limit" : %d, "stop" : "%s"}' % (max(tasks)[0], limit, stop)
        db.sql_update("UPDATE `common_config` SET `value` = '%s' WHERE `key` = 'gongshang_weixin_crawler'" % (common_config))

if __name__ == '__main__':
    logging.config.fileConfig("conf_log.conf")
    logging.getLogger().info("start main")
    logging.getLogger().info("start to init mysql connection pool")
    db.sql_connect("default.ini", "spider_con_config")
    work()
    logging.getLogger().info("end main exit")
