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


#获得查询列表
def getSearchList(proxies, company_name):
    target_url = "http://wx.qixin007.com/search/" + quote(company_name.encode('utf-8')) +".html?province="
    #print target_url
    headers = {
        "Host":"wx.qixin007.com",
        "Accept":"application/json, text/plain, */*",
        "Origin":"http://wx.qixin007.com",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat",
        "Accept-Language":"zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
        "Cookie":"aliyungf_tc=AQAAAFfyHEQB9QIAFnKm3ytM0AL0pXJg; sid=s%3AhWqG5wBQ7-PVzdcpcMLnEiQpdSSZM83f.k0nBBWu%2BzAvqF%2FgSK8UyN98tUpwB6KJYYJno%2FkJSmGU; Hm_lvt_020971628e81872ff6d45847a9a8f09c=1468981082; Hm_lpvt_020971628e81872ff6d45847a9a8f09c=1468984231",
        "Referer":"",
    }
    status = 0
    #headers['Referer'] = "http://wx.qixin007.com/search/" + quote(company_name.encode("utf-8")) + ".html?code=011Keyin0hKU6e1IpChn0H2uin0Keyin&state=1"
    resp = getResponse(target_url=target_url, proxies=proxies, headers=headers)
    logging.getLogger().info("Code: %d" % resp['code'])

    if resp['code'] != 0:
        status = -1
        return [], status

    if resp['content'].text == []:
        logging.getLogger().error("IP is sealed!")
    elif resp['content'].text[9] == 0:
        logging.getLogger().info("Content is null!")

    return resp['content'].text, status


#解析查询列表
def parseSearchList(json_str):
    #json_str = '{"total":6,"num":6,"searchtime":0.097345,"items":[{"name":"<em>上海合合信息科技发展有限公司</em>","start_date":"2006-08-08","v_count":"13844","oper_name":"镇立新","c_count":"270","score":"841.000","domain":"<em>科技</em>推广和应用服务业","address":"<em>上海</em>市杨浦区国定路335号3号楼7楼B区","email":"xuemei_liu@intsig....","phone":"...","status":"存续","status_code":"A","eid":"e8cdf5e0-97ad-4e1e-a8e4-29358f8a9866","source":"9","logo_url":"http://qxb-img.oss-cn-hangzhou.aliyuncs.com/logo/e8cdf5e0-97ad-4e1e-a8e4-29358f8a9866.jpg","reg_capi":"1,000.000000 万人民币","province":"SH","city_code":"3101","match_items":[{"match_field":"企业名称","match_value":"<em>上海合合信息科技发展有限公司</em>"}]},{"name":"<em>上海</em>脉<em>合信息科技发展有限公司</em>","start_date":"2007-03-19","v_count":"72","oper_name":"全文娟","c_count":"0","score":"580.619","domain":"<em>科技</em>推广和应用服务业","address":"虹梅路3321弄100支弄54号602","email":"无","phone":"021-...","status":"存续","status_code":"A","eid":"1408ff67-1fb0-4b46-9a51-8077a34fce42","source":"9","logo_url":"","reg_capi":"100.000000 万人民币","province":"SH","city_code":"3101","match_items":[{"match_field":"企业名称","match_value":"<em>上海</em>脉<em>合信息科技发展有限公司</em>"}]},{"name":"苏州贝尔塔数据<em>技</em>术<em>有限公司</em>","start_date":"2014-05-28","v_count":"23020","oper_name":"陈飒","c_count":"379","score":"561.184","domain":"软件和<em>信息</em>技术服务业","address":"苏州市工业园区东长路88号A2幢2层203室","email":"laura@bertadata.com","phone":"0512-62806871","status":"在业","status_code":"A","eid":"2c861a47-f7f2-457c-9f62-c96a58d9743f","source":"9","logo_url":"http://qxb-img.oss-cn-hangzhou.aliyuncs.com/logo/2c861a47-f7f2-457c-9f62-c96a58d9743f.jpg","reg_capi":"1,000 万人民币","province":"JS","city_code":"3205","match_items":[{"match_field":"股东","match_value":"<em>上海合合信息科技发展有限公司</em> "}]},{"name":"宁波长汇嘉<em>信</em>投资中心（<em>有限合</em>伙）","start_date":"2014-04-28","v_count":"362","oper_name":"","c_count":"0","score":"0.000","domain":"资本市场服务","address":"北京市朝阳区建国门外大街2号银泰中心C座4702室","email":"hehuoren@maifund.com","phone":"010-85876288","status":"存续","status_code":"A","eid":"24bd952a-b84e-4265-906e-d558d544f69d","source":"9","logo_url":"","reg_capi":"-","province":"ZJ","city_code":"3302","match_items":[{"match_field":"股东","match_value":"<em>上海合合信息科技发展有限公司</em> "}]},{"name":"<em>上海</em>屹通<em>信息科技发展有限公司合</em>肥分公司","start_date":"2013-11-15","v_count":"0","oper_name":"章祺","c_count":"0","score":"0.000","domain":"<em>科技</em>推广和应用服务业","address":"合肥市高新区梦园小区留澜居2栋401","email":"yt@yitong....","phone":"...","status":"存续","status_code":"A","eid":"dbe854ae-6d45-412d-9216-dd1a77ccb634","source":"9","logo_url":"","reg_capi":"-","province":"AH","city_code":"3401","match_items":[{"match_field":"企业名称","match_value":"<em>上海</em>屹通<em>信息科技发展有限公司合</em>肥分公司"}]},{"name":"<em>上海</em>安广<em>信息科技发展有限</em>责任<em>公司合</em>肥办事处","start_date":"2003-10-13","v_count":"0","oper_name":"赵明","c_count":"0","score":"0.000","domain":"软件和<em>信息</em>技术服务业","address":"安徽省合肥市濉溪路142号深圳花园14幢602室.","email":"","phone":"","status":"吊销","status_code":"N","eid":"862113a1-78b8-423f-a229-c2c21f030735","source":"9","logo_url":"","reg_capi":"-","province":"AH","city_code":"3401","match_items":[{"match_field":"企业名称","match_value":"<em>上海</em>安广<em>信息科技发展有限</em>责任<em>公司合</em>肥办事处"}]}]}'
    start = 0
    results = []
    try:
        content_str = json.loads(json_str)
        totalnum = content_str['num']
        items = content_str['items']
    except Exception as e:
        logging.getLogger().error("List_parser is failed!")
        logging.getLogger().exception(e)
        return results

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

def storeSearchList(datas, id, status):
    #print datas
    if len(datas) == 0:
        if status == -1:
            db.sql_update("UPDATE `common_company_name` SET `qixin_weixin_status` = 3 WHERE `id` = %d" % id)
        else:
            db.sql_update("UPDATE `common_company_name` SET `qixin_weixin_status` = 2 WHERE `id` = %d" % id)
        return
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
    except Exception as e:
        logging.getLogger().error('error to insert into `qixin_list`')
        logging.exception(e)


def work():
    site = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'qixin_weixin_site'")
    while True:
        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'qixin_weixin_crawler'")
        value = json.loads(common_config)
        start = value['start']
        limit = value['limit']
        stop = value['stop']
        if stop == "True":
            logging.getLogger().info("Start to sleep 5 second.")
            time.sleep(5)
            continue

        tasks = db.sql_fetch_rows("SELECT `id`, `area_code`, `name`, `qixin_weixin_status`FROM `common_company_name` WHERE `id` >= %d AND `id` < %d" % (start, start + limit))
        max_id = -1
        if tasks != None:
            for task in tasks:
                logging.getLogger().info("Start to crawler %d : %s" % (task[0], task[2] ))
                id = task[0]
                max_id = max(id, max_id)

                if task[3] != 0:
                    logging.getLogger().info("End to crawler %d" % task[0])
                    continue

                company_name = task[2]
                datas = []
                start_time = time.time()
                proxies = getProxy(site)
                #print company_name
                logging.getLogger().info("Start to get the list")
                (html, status) = getSearchList(proxies, company_name)
                #logging.getLogger().info("The list is : %s" % html)
                if status == 0:
                    logging.getLogger().info("Start to parse the list")
                    datas = parseSearchList(html)
                #logging.getLogger().info("Parser the list : %s" % datas)
                storeSearchList(datas, id, status)
                logging.getLogger().info("End to crawl the list %d" % task[0])
                sleep_sec = random.random() + 2.5;
                logging.getLogger().info("sleep seconds %.3f" % sleep_sec)
                time.sleep(sleep_sec)
        else:
            logging.getLogger().info("No keyword could be found, start to sleep 10 second.")
            time.sleep(10)
            continue

        common_config = db.sql_fetch_one_cell("SELECT `value` FROM `common_config` WHERE `key` = 'qixin_weixin_crawler'")
        value = json.loads(common_config)
        stop = value['stop']
        common_config = '{"start" : %d, "limit" : %d, "stop" : "%s"}' % (max_id + 1, limit, stop)
        db.sql_update("UPDATE `common_config` SET `value` = '%s' WHERE `key` = 'qixin_weixin_crawler'" % (common_config))

if __name__ == '__main__':
    logging.config.fileConfig("conf_log.conf")
    logging.getLogger().info("start main")
    logging.getLogger().info("start to init mysql connection pool")
    db.sql_connect("default.ini", "spider_con_config")
    try:
        work()
    except Exception as e:
        logging.getLogger().error("exit inormal")
        logging.getLogger().exception(e);

    logging.getLogger().info("end main exit")
