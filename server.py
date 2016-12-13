# coding: utf-8

import tornado.ioloop
import tornado.web
import struct
import torndb
import time
import geoip2.database

import config
from Bastion import _test

MATCH_CONTENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
MATCH_CONTENT += "<wml>"
MATCH_CONTENT += "<card>"
MATCH_CONTENT += "<Ccmd_cust>zp[id]</Ccmd_cust>"
MATCH_CONTENT += "<Cnum_cust>[mobile]</Cnum_cust>"
MATCH_CONTENT += "<filter1_cust></filter1_cust>"
MATCH_CONTENT += "<filter2_cust></filter2_cust>"
MATCH_CONTENT += "<Creconfirm_cust></Creconfirm_cust>"
MATCH_CONTENT += "<fee></fee>"
MATCH_CONTENT += "<autofee></autofee>"
MATCH_CONTENT += "<feemode>-2</feemode>"
MATCH_CONTENT += "</card>"
MATCH_CONTENT += "</wml>"

TEST_CONTENT =  "";

CONSTANT = 0 
MATCH_FLOW_LIMIT_PER_MINUTE = {'minute':0,'count':0} 

def modifyGlobal():  
    global CONSTANT  
    print(CONSTANT)  
    CONSTANT += 1 

class MatchHandler(tornado.web.RequestHandler):
    def get(self):
        dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
        sql = 'SELECT imsi FROM `imsi_users` WHERE id = %s '
        _recordRsp = dbConfig.get(sql,self.get_argument('id'))
        if _recordRsp!=None:
            sql = "update `imsi_users` set mobile=%s where id = %s"    
            dbConfig.update(sql,self.get_argument('mobile'),self.get_argument('id')) 

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")
        modifyGlobal()  
    def post(self, *args, **kwargs):
        _begin_time = int(round(time.time() * 1000))
        reqInfo = {}
        reqInfo["imsi"] = self.request.body[64:80]
        reqInfo["ip"] = self.request.headers["X-Real-IP"]
        insert_req_log(reqInfo)
        _test_imsi_info = check_test_imsi(reqInfo["imsi"]);
        if _test_imsi_info == None:
            #process normal user
            _rsp_content = get_imsi_register_response(reqInfo["imsi"])
            self.write(TEST_CONTENT)
        else:
            self.write(get_test_response(_test_imsi_info));
        print "tcd spent:"+str(int(round(time.time() * 1000))-_begin_time)
            
def get_imsi_register_response(_imsi):
    _return = "";
    _imsi=filter(str.isdigit, _imsi)
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    _sql = 'SELECT id,imsi,mobile,matchCount FROM `imsi_users` WHERE imsi = %s '
    _recordRsp = dbConfig.get(_sql, _imsi) 
    if _recordRsp==None:
        _sql = 'insert into `imsi_users` (imsi,insertTime) value (%s,%s)'
        dbConfig.insert(_sql,_imsi,time.time())
        _sql = "SELECT LAST_INSERT_ID() as id"
        _recordRsp = dbConfig.get(_sql) 
        if _recordRsp!=None:
            _return = MATCH_CONTENT
            _return = _return.replace('[id]', str(_recordRsp['id'])).replace('[mobile]', get_system_parameter_from_db("matchMobile"))
    return _return

def get_system_parameter_from_db(_title):
    _return = '';
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    _sql = 'SELECT detail FROM `system_configs` WHERE title = %s '
    _recordRsp = dbConfig.get(_sql, _title) 
    if _recordRsp!=None:
        _return = _recordRsp['detail']
    return _return

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/tcd/", MainHandler),
        (r"/match/", MatchHandler),
    ])

def get_test_response(_imsi_info):
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
    _recordRsp = dbConfig.get(sql, _imsi_info['imsi'],_imsi_info['testStatus'])
    if _recordRsp==None:
        sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
        _recordRsp = dbConfig.get(sql,"def",_imsi_info['testStatus'])
    else:
        print(_recordRsp)
    return _recordRsp['response'].replace("IMSIimsi",_imsi_info['imsi']);
    
def check_test_imsi(imsi):
    print('before:'+imsi);
    imsi=filter(str.isdigit, imsi)
    print('after filter:'+imsi);
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    sql = 'SELECT imsi,testStatus FROM test_imsis WHERE imsi = %s'
    _record = dbConfig.get(sql, imsi)
    return _record

def insert_req_log(_reqInfo):
    print('_reqInfo.imsi:'+_reqInfo["imsi"]);
    imsi=filter(str.isdigit, _reqInfo["imsi"])
    print('imsi:'+imsi);
    reader = geoip2.database.Reader(config.GLOBAL_SETTINGS['geoip2_db_file_path'])
    response = reader.city(_reqInfo["ip"])
    dbLog=torndb.Connection(config.GLOBAL_SETTINGS['log_db']['host'],config.GLOBAL_SETTINGS['log_db']['name'],config.GLOBAL_SETTINGS['log_db']['user'],config.GLOBAL_SETTINGS['log_db']['psw'])
    sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`) values (%s,%s,%s,%s,%s,%s)'
    dbLog.insert(sql,int(round(time.time() * 1000)),1,imsi,_reqInfo["ip"],response.subdivisions.most_specific.name,response.city.name)
    return 

if __name__ == "__main__":
    app = make_app()
    app.listen(config.GLOBAL_SETTINGS['port'])
    tornado.ioloop.IOLoop.current().start()