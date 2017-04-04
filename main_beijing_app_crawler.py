#coding=utf-8

import sys, logging.config, os, time
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../")

import requests, io, ConfigParser, json
import app.common.db as db
from requests.exceptions import ProxyError, ConnectTimeout, ReadTimeout
from urllib import quote


#获得代理IP
def getProxy(site):
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("default.ini", encoding="utf8"))
    host = json.loads(config.get("default", "proxies_config"))

    proxies = {
        'http': '',
    }

    return {'http':'110.186.9.67:80'}

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

    return {'code' : 0, 'content' : r}


#获得查询列表
def getSearchList(proxies, company_name, province_name):
    target_url = 'http://211.94.187.236:8088/android/androidAction!tb1.dhtml?clear=true&ent_name=' + quote(company_name.encode('utf-8')) + '&net_type=ios&pageNo=1&reg_no=' + quote(company_name.encode('utf-8')) + '&verify_code=EEE0C58C332BB1B555A88A1997277618'
    headers = {
        "Host":"211.94.187.236:8088",
        "Accept":"*/*",
        "Accept-Language":"zh-cn",
        "User-Agent":"credit/1.10.2 CFNetwork/758.3.15 Darwin/15.4.0",
    }
    resp = getResponse(target_url=target_url, proxies=proxies, headers=headers)

    print resp

    print target_url
    print headers
    print resp['content'].headers

    return resp['content'].text


#解析查询列表
def parseSearchList(json_str):
    #json_str = '{"total":6,"num":6,"searchtime":0.097345,"items":[{"name":"<em>上海合合信息科技发展有限公司</em>","start_date":"2006-08-08","v_count":"13844","oper_name":"镇立新","c_count":"270","score":"841.000","domain":"<em>科技</em>推广和应用服务业","address":"<em>上海</em>市杨浦区国定路335号3号楼7楼B区","email":"xuemei_liu@intsig....","phone":"...","status":"存续","status_code":"A","eid":"e8cdf5e0-97ad-4e1e-a8e4-29358f8a9866","source":"9","logo_url":"http://qxb-img.oss-cn-hangzhou.aliyuncs.com/logo/e8cdf5e0-97ad-4e1e-a8e4-29358f8a9866.jpg","reg_capi":"1,000.000000 万人民币","province":"SH","city_code":"3101","match_items":[{"match_field":"企业名称","match_value":"<em>上海合合信息科技发展有限公司</em>"}]},{"name":"<em>上海</em>脉<em>合信息科技发展有限公司</em>","start_date":"2007-03-19","v_count":"72","oper_name":"全文娟","c_count":"0","score":"580.619","domain":"<em>科技</em>推广和应用服务业","address":"虹梅路3321弄100支弄54号602","email":"无","phone":"021-...","status":"存续","status_code":"A","eid":"1408ff67-1fb0-4b46-9a51-8077a34fce42","source":"9","logo_url":"","reg_capi":"100.000000 万人民币","province":"SH","city_code":"3101","match_items":[{"match_field":"企业名称","match_value":"<em>上海</em>脉<em>合信息科技发展有限公司</em>"}]},{"name":"苏州贝尔塔数据<em>技</em>术<em>有限公司</em>","start_date":"2014-05-28","v_count":"23020","oper_name":"陈飒","c_count":"379","score":"561.184","domain":"软件和<em>信息</em>技术服务业","address":"苏州市工业园区东长路88号A2幢2层203室","email":"laura@bertadata.com","phone":"0512-62806871","status":"在业","status_code":"A","eid":"2c861a47-f7f2-457c-9f62-c96a58d9743f","source":"9","logo_url":"http://qxb-img.oss-cn-hangzhou.aliyuncs.com/logo/2c861a47-f7f2-457c-9f62-c96a58d9743f.jpg","reg_capi":"1,000 万人民币","province":"JS","city_code":"3205","match_items":[{"match_field":"股东","match_value":"<em>上海合合信息科技发展有限公司</em> "}]},{"name":"宁波长汇嘉<em>信</em>投资中心（<em>有限合</em>伙）","start_date":"2014-04-28","v_count":"362","oper_name":"","c_count":"0","score":"0.000","domain":"资本市场服务","address":"北京市朝阳区建国门外大街2号银泰中心C座4702室","email":"hehuoren@maifund.com","phone":"010-85876288","status":"存续","status_code":"A","eid":"24bd952a-b84e-4265-906e-d558d544f69d","source":"9","logo_url":"","reg_capi":"-","province":"ZJ","city_code":"3302","match_items":[{"match_field":"股东","match_value":"<em>上海合合信息科技发展有限公司</em> "}]},{"name":"<em>上海</em>屹通<em>信息科技发展有限公司合</em>肥分公司","start_date":"2013-11-15","v_count":"0","oper_name":"章祺","c_count":"0","score":"0.000","domain":"<em>科技</em>推广和应用服务业","address":"合肥市高新区梦园小区留澜居2栋401","email":"yt@yitong....","phone":"...","status":"存续","status_code":"A","eid":"dbe854ae-6d45-412d-9216-dd1a77ccb634","source":"9","logo_url":"","reg_capi":"-","province":"AH","city_code":"3401","match_items":[{"match_field":"企业名称","match_value":"<em>上海</em>屹通<em>信息科技发展有限公司合</em>肥分公司"}]},{"name":"<em>上海</em>安广<em>信息科技发展有限</em>责任<em>公司合</em>肥办事处","start_date":"2003-10-13","v_count":"0","oper_name":"赵明","c_count":"0","score":"0.000","domain":"软件和<em>信息</em>技术服务业","address":"安徽省合肥市濉溪路142号深圳花园14幢602室.","email":"","phone":"","status":"吊销","status_code":"N","eid":"862113a1-78b8-423f-a229-c2c21f030735","source":"9","logo_url":"","reg_capi":"-","province":"AH","city_code":"3401","match_items":[{"match_field":"企业名称","match_value":"<em>上海</em>安广<em>信息科技发展有限</em>责任<em>公司合</em>肥办事处"}]}]}'
    content_str = json.loads(json_str)
    totalnum = content_str['num']
    start = 0
    items = content_str['items']
    results = []
    while start < totalnum:
        temp_res = []
        temp_compinfo = items[start]
        company_name = temp_compinfo['name'].replace("<em>", "").replace("</em>", "")
        eid = temp_compinfo['eid']
        temp_res.append(company_name)
        temp_res.append(eid)
        temp_res.append(str(temp_compinfo).replace("<em>", "").replace("</em>", ""))
        results.append(temp_res)
        start += 1
    return results

def storeSearchList(datas, id):
    #print datas
    if len(datas) == 0:
        db.sql_update("UPDATE `common_company_name` SET `qixin_weixin_status` = 2 WHERE `id` = %d" % id)
    else:
        db.sql_update("UPDATE `common_company_name` SET `qixin_weixin_status` = 1 WHERE `id` = %d" % id)
    insert_stmt = "INSERT INTO `qixin_list`(company_name, eid, web_extra, crawl_time) VALUES(%s, %s, %s, NOW())"
    insert_data = []
    for item in datas:
        insert_temp = []
        company_name = item[0]
        eid = item[1]
        web_extra = item[2]
        exist_code = db.sql_fetch_one("SELECT * FROM `qixin_list` WHERE `eid` = '%s'" % eid)
        if exist_code == None:
            insert_temp.append(company_name)
            insert_temp.append(eid)
            insert_temp.append(json.dumps(eval(web_extra), encoding='utf-8', ensure_ascii=False))
            insert_data.append(insert_temp)

    try:
        db.sql_insert_many(insert_stmt, insert_data)
    except Exception, e:
        logging.error('error to insert into `qixin_list`')
        logging.exception(e)


#获得工商基本信息
def getBasicInfo(proxies):
    company_exists = db.sql_fetch_one("SELECT `id`, `eid` FROM `qixin_list` WHERE `crawl_status` = 0 LIMIT 1")
    if company_exists == None:
        return None
    eid = company_exists[1]
    id = company_exists[0]
    target_url = "http://wx.qixin007.com/company/" + str(eid) + ".html"
    print target_url
    headers = {
        "Host":"wx.qixin007.com",
        "Accept":"application/json, text/plain, */*",
        "Origin":"http://wx.qixin007.com",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Referer":"http://wx.qixin007.com/company/ac6a078e-48f2-46ab-8bbf-1d8c2c63c9e5.html",
        "Cookie":"aliyungf_tc=AQAAAFfyHEQB9QIAFnKm3ytM0AL0pXJg; sid=s%3AhWqG5wBQ7-PVzdcpcMLnEiQpdSSZM83f.k0nBBWu%2BzAvqF%2FgSK8UyN98tUpwB6KJYYJno%2FkJSmGU; Hm_lvt_020971628e81872ff6d45847a9a8f09c=1468981082; Hm_lpvt_020971628e81872ff6d45847a9a8f09c=1468984231",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
    }
    resp = getResponse(target_url, proxies, headers)
    print resp
    src = resp['content'].text
    db.sql_update("UPDATE `qixin_list` SET `src_basicinfo` = %s, `crawl_status` = 1, `crawl_time` = NOW() WHERE `id` = %s", (src, id))


#获取查询关键字
def getSearchKey(start, limit):
    res = db.sql_fetch_rows("SELECT `id`, `name` FROM `common_company_name` WHERE `area_code` = 110000 AND `beijing_app_status` = 0 LIMIT %d" % (limit))
    return res



def work():
    site = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'beijing_app_site'")
    while True:
        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'beijing_app_crawler'")
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
                logging.getLogger().info(company_name)
                html = getSearchList(proxies, company_name, areacode)
                print html
                #datas = parseSearchList(html)
                #storeSearchList(datas, id)
                logging.getLogger().info("end to crawler %d" % task[0])
                #getBasicInfo(proxies)
                time.sleep(2)
                #time.sleep(10)


        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'beijing_app_crawler'")
        value = json.loads(common_config)
        stop = value['stop']
        if tasks == None:
            common_config = '{"start" : %d, "limit" : %d, "stop" : "%s"}' % (start + limit, limit, stop)
        else:
            common_config = '{"start" : %d, "limit" : %d, "stop" : "%s"}' % (max(tasks)[0], limit, stop)
        db.sql_update("UPDATE `common_config` SET `value` = '%s' WHERE `key` = 'beijing_app_crawler'" % (common_config))
        #break
F
if __name__ == '__main__':
    logging.config.fileConfig("conf_log.conf")
    logging.getLogger().info("start main")
    logging.getLogger().info("start to init mysql connection pool")
    db.sql_connect("default.ini", "spider_con_config")
    #work()
    #proxies = getProxy('')
    #print getSearchList(proxies, '上海合合信息科技', 'SH')
    #logging.getLogger().info("end main exit")
