#coding=utf-8

import sys, logging.config
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../../../")

import requests, io, ConfigParser, json, random,time, urllib, threading;

from requests.exceptions import ProxyError, ConnectTimeout, ReadTimeout
import app.common.db as db

config_sogou_list = None;
adapter = None;
site ="tianyancha_web"

def get_common_header():
    common_header = {};
    common_header["Accept"]="application/json, text/plain, */*";
    common_header["Accept-Encoding"]="gzip, deflate, sdch";
    common_header["Accept-Language"]="zh-CN,zh;q=0.8,en;q=0.6";
    common_header["Cache-Control"]="no-cache";
    common_header["Pragma"]="no-cache";
    common_header["User-Agent"]="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36";
    ip_suffix = str(random.randint(2,233));
    # common_header["HTTP_X_FORWARDED_FOR"]="171.88.115." + ip_suffix;
    # common_header["x-forwarded-for"]="171.88.115." + ip_suffix;
    # common_header["HTTP_VIA"]="171.88.115." + ip_suffix;
    # common_header["HTTP_CLIENT_IP"]="171.88.115." + ip_suffix;
    # common_header["HTTP_SP_HOST"]="171.88.115." + ip_suffix;
    # common_header["WL-Proxy-Client-IP"]="171.88.115." + ip_suffix;
    # common_header["Proxy-Client-IP"]="171.88.115." + ip_suffix;
    return common_header;

def getProxy(site):
    # return None;
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

def parse_tokenjs(js_code_asc, key_word):
    out_str = u""
    if  not isinstance(js_code_asc, (tuple, list)):
        js_code_asc = js_code_asc.strip().split(",");

    for ite in js_code_asc:
        out_str += chr(int(ite));
    # logging.getLogger().info("out_str:%s" % (out_str));
    token_start = out_str.index("token=");
    token_end = out_str.index(";");
    token_str = out_str[token_start + len("token="):token_end].strip();
    utm_start = out_str.index(ur"return'", token_end);
    utm_end = out_str.index(ur"'", utm_start + len("return'") + 1);
    utm_list = out_str[utm_start + len("return'") : utm_end ].split(",");
    logging.getLogger().info("utm_list :%s  " % (",".join(utm_list), ));
    utm_str = u"";
    global config_sogou_list ;
    config_sogou_idx = int(str(ord(key_word[0]))[1])
    logging.getLogger().info("window.$sogou$:%s  " % (",".join(config_sogou_list[config_sogou_idx]), ));
    # logging.getLogger().info("key word:%s  sogou :%s" % (key_word, json.dumps(config_sogou_list[config_sogou_idx])));
    for idx in utm_list :
        utm_str += config_sogou_list[config_sogou_idx][int(idx)];
    logging.getLogger().info("token:%s  utm:%s" % (token_str, utm_str));
    return {"token" : token_str, "_utm": utm_str};

def do_http_get_namelist(company_name):
    company_name = company_name.strip();
    with requests.session() as session:
        result = [False,[]];
        session.mount('http://', adapter)
        company_name_urlencode = urllib.urlencode({"a":company_name}).replace("a=", "", 1);
        url = ur"http://www.tianyancha.com/tongji/" + company_name_urlencode +  ur".json?random=" + str(int(time.time() * 1000));
        logging.getLogger().info("url: %s" % url);
        header_map = get_common_header();
        header_map["Referer"] = "http://www.tianyancha.com/search/" + company_name_urlencode;
        header_map["Host"] = "www.tianyancha.com";
        # proxy = getProxy(site);
        proxy = None;
        response = session.get(url,proxies=proxy,  headers = header_map) ;
        logging.getLogger().info("url: %s response code: %s  content: **%s**" % (url, response.status_code, response.text) );
        if response.status_code != 200:
            result[0] = False;
            return result;
        result_json = json.loads(response.text)
        if result_json.get("state") != "ok":
            result[0] = False;
            return result;
        js_code_asc = result_json["data"]["v"]
        cookie_map = parse_tokenjs(js_code_asc, company_name);
        logging.getLogger().info("pared cookie : %s" % (json.dumps(cookie_map)) );
        cookie_map['_pk_ses.1.e431'] = "*"
        url = ur"http://www.tianyancha.com/search/" + company_name_urlencode + ".json?&pn=1" ;
        response = session.get(url,proxies=proxy,  headers = header_map, cookies = cookie_map) ;
        logging.getLogger().info("url: %s response code: %s  content: **%s**" % (url, response.status_code, response.text) );
        if response.status_code != 200:
            result[0] = False;
            return result;
        result_json = json.loads(response.text);
        if "state" in result_json and  result_json["state"]=="ok" and "data" in result_json :
            result = [True, result_json["data"]]
            if result[1] == None:
                result[0] = False;
        elif "state" in result_json and  result_json["message"]==u"无数据":
            result = [True, []];
        return result;

def do_http_get_company(company_id):
    company_id = str(company_id).strip();
    result = [False,[]];
    try:
        with requests.session() as session:
            session.mount('http://', adapter)
            company_name_urlencode = urllib.urlencode({"a":company_id}).replace("a=", "", 1);
            url = ur"http://www.tianyancha.com/tongji/" + company_name_urlencode +  ur".json?random=" + str(int(time.time() * 1000));
            logging.getLogger().info("url: %s" % url);
            header_map = get_common_header();
            header_map["Referer"] = "http://www.tianyancha.com/company/" + company_name_urlencode;
            header_map["Host"] = "www.tianyancha.com";
            header_map["Tyc-From"] = "normal";
            proxy = getProxy(site);
            # proxy = None;
            response = session.get(url,proxies=proxy,  headers = header_map) ;
            logging.getLogger().info("url: %s response code: %s  content: **%s**" % (url, response.status_code, response.text) );
            result_json = json.loads(response.text)
            if result_json.get("state") != "ok":
                result[0] = False;
                return result;
            js_code_asc = result_json["data"]["v"]
            cookie_map = parse_tokenjs(js_code_asc, company_id);
            cookie_map['_pk_ses.1.e431'] = "*"
            url = ur"http://www.tianyancha.com/company/" + company_name_urlencode + ".json" ;
            response = session.get(url,proxies=proxy,  headers = header_map, cookies = cookie_map) ;
            logging.getLogger().info("url: %s response code: %s  content: **%s**" % (url, response.status_code, response.text) );
            if response.status_code != 200:
                result[0] = False;
                return result;
            result_json = json.loads(response.text);
            if "state" in result_json and  result_json["state"]=="ok" and "data" in result_json :
                result = [True, result_json["data"]];
                if result[1] == None:
                    result[0] = False;
            return result;
    except Exception,err:
        return result;
        logging.getLogger().exception(err);

def fill_company_summary(comany_id,  company_sumary) :
    comany_id = long(comany_id);
    select_sql = "select id from tianyancha_web_company where `webid`=%s";
    row = db.sql_fetch_one(select_sql, (comany_id, ));
    company_name = company_sumary["name"].replace(r"<em>", "").replace(r"</em>", "");
    src_summary = json.dumps(company_sumary, ensure_ascii=False);
    if row == None:
        sql_insert = "insert into tianyancha_web_company(webid, company_name, src_summary, crawl_time) values(%s, %s, %s, now())";
        db.sql_insert(sql_insert, (comany_id, company_name, src_summary , ));
        logging.getLogger().info("insert new company: %s name :%s" % (comany_id, company_name,))
    else:
        sql_update = "update  tianyancha_web_company set   src_summary=%s where id=%s";
        db.sql_update(sql_update, (src_summary, row[0] ));
        logging.getLogger().info("update exists company: %s name :%s" % (comany_id, company_name,))

def fill_compay_detail(table_id, comapy_detail):
    company_name = comapy_detail["baseInfo"]["name"];
    src_detail = json.dumps(comapy_detail, ensure_ascii=False);
    sql_update = "update  tianyancha_web_company set  company_name=%s, src_detail=%s where id=%s";
    db.sql_update(sql_update, (company_name,src_detail, table_id ));

def update_qixin_status(id, status):
    sql_update = "update  qixin_list set  `tianyancha_satus`=%s where id=%s";
    db.sql_update(sql_update, (status,  id ));

def update_status(table, field, id, status):
    sql_update = "update  " + table+ "  set  `" + field + "`=%s where id=%s";
    db.sql_update(sql_update, (status,  id ));

def crawl_namelist():
    db.sql_connect("default.ini", "spider_con_config");
    while True:
        logging.getLogger().info("start to crawl company namelist " )
        # sql_select = "select `id`, `company_name` from qixin_list where tianyancha_satus=0 limit 500 ";
        # rows = db.sql_fetch_rows(sql_select);
        # rows = ([] if rows == None else rows);
        resp = requests.get("http://10.51.1.201:3351/getKeywords");
        resp_json = resp.json();
        rows =[[ ite["id"], ite["keyword"]] for ite in resp_json]
        for row in rows :
            try:
                # row[1] = u"河南兴发装饰工程有限公司";
                sleep_seconds = 10 + random.random() * 10;
                logging.getLogger().info("start to sleep seconds %.2f" % sleep_seconds);
                time.sleep(sleep_seconds)
                logging.getLogger().info("start to crawl company keyword: %s" % row[1] )
                (status, company_list) = do_http_get_namelist(row[1]) ;
                if status == False:
                    update_status("common_keyword",  "status",   row[0], 2);
                    logging.getLogger().info("failed crawl company keyword: %s" % row[1] )
                    continue;
                logging.getLogger().info("get company size : %s crawl company keyword: %s" %(len(company_list),  row[1] )) ;
                for ite_compay in company_list:
                    company_id = ite_compay["id"] ;
                    logging.getLogger().info("start to fill compaynid: %s name: %s" %( company_id, ite_compay["name"])) ;
                    fill_company_summary(company_id,  ite_compay);
                update_status("common_keyword",  "status",   row[0], 1);

            except Exception, err:
                logging.getLogger().info("failed deal id:%s company key word:%s" % (row[0], row[1]));
                logging.getLogger().exception(err);
        sleep_minutes = 3 + random.random()*2;
        logging.getLogger().info("start to sleep minutes %.2f" % sleep_minutes);
        time.sleep(sleep_seconds * 60)

def crawl_company_detail():
    db.sql_connect("default.ini", "spider_con_config");
    while True:
        logging.getLogger().info("start to crawl comany detail");
        sql_select = "select `id`, webid from tianyancha_web_company where crawl_status = 0 limit 500 ";
        rows = db.sql_fetch_rows(sql_select);
        for row in rows:
            try:
                (status, company_detail) = do_http_get_company(row[1]);
                if status == False:
                    update_status("tianyancha_web_company",  "crawl_status",   row[0], 2);
                    continue;
                fill_compay_detail(row[0],  company_detail );
                update_status("tianyancha_web_company",  "crawl_status",   row[0], 1);
                sleep_seconds = 40 + random.random()*10;
                logging.getLogger().info("start to sleep seconds %.2f" % sleep_seconds);
                time.sleep(sleep_seconds)
            except Exception, err:
                logging.getLogger().info("failed deal id:%s company name:%s" % (row[0], row[1]));
                logging.getLogger().exception(err);
        sleep_seconds = 1 + random.random()*2;
        logging.getLogger().info("start to sleep minutes %.2f" % sleep_seconds);
        time.sleep(sleep_seconds * 60)

def config_init():
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("./default.ini", encoding="utf8"))
    global config_sogou_list ;
    tmp_value = json.loads(config.get("default", "config_sogou_list"));
    global config_sogou_list;
    if isinstance(tmp_value, (tuple, list)):
        config_sogou_list = tmp_value;
    else:
        config_sogou_list = []
        tmp_value = tmp_value.split(",");
        for idx in range(10):
            start = idx*37;
            end = start + 37;
            config_sogou_list.append(tmp_value[start : end ]);
    logging.getLogger().info("config_sogou str: %s" %  json.dumps(config_sogou_list));
    tmp_value = json.loads(config.get("default", "adapter_config").strip());
    global adapter;
    adapter = requests.adapters.HTTPAdapter(**tmp_value)
    logging.getLogger().info("config_sogou str: %s" % config_sogou_list);

if __name__ == '__main__':
    logging.config.fileConfig("conf_log.conf")
    logging.getLogger().info("start main" )
    logging.getLogger().info("start to init mysql connection pool" )
    # db.sql_connect("default.ini", "spider_con_config")
    config_init();
    # company_name = u"济南星火技术发展有限公司";
    # do_http_get_namelist(company_name)
    # do_http_get_company(23827952)

    # js_asc_str = "33,102,117,110,99,116,105,111,110,40,110,41,123,100,111,99,117,109,101,110,116,46,99,111,111,107,105,101,61,39,116,111,107,101,110,61,57,54,50,99,57,99,51,97,55,57,52,102,52,56,52,51,97,52,102,48,48,102,56,99,48,51,53,57,100,49,56,56,59,112,97,116,104,61,47,59,39,59,110,46,119,116,102,61,102,117,110,99,116,105,111,110,40,41,123,114,101,116,117,114,110,39,49,44,48,44,49,44,48,44,49,57,44,50,56,44,49,52,44,50,57,44,52,44,51,50,44,49,52,44,49,56,44,50,57,44,50,55,44,51,50,44,50,56,44,49,57,44,49,56,44,49,56,44,51,50,44,50,56,44,51,48,44,50,57,44,55,44,51,44,55,44,51,44,49,57,44,51,50,44,51,49,44,52,44,49,39,125,125,40,119,105,110,100,111,119,41,59"
    # key_word = u"黄山市黟县宏";
    # parse_tokenjs(js_asc_str, key_word);
    #
    # work();
    logging.getLogger().info("start thread crawl name list for tianyancha" )
    threading.Thread(target=crawl_namelist).start();
    logging.getLogger().info("start thread crawl company detail for tianyancha" )
    # threading.Thread(target=crawl_company_detail).start();
    logging.getLogger().info("end main exit" )

