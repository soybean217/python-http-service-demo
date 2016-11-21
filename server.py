# coding: utf-8

import tornado.ioloop
import tornado.web
import struct
import torndb
import time
import geoip2.database

import config
from Bastion import _test

# TEST_CONTENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
# TEST_CONTENT += "<wml>"
# TEST_CONTENT +="<card>"
# TEST_CONTENT +="<Ccmd_cust>3</Ccmd_cust>"
# TEST_CONTENT +=  "<Cnum_cust>13590100473</Cnum_cust>"
# TEST_CONTENT +=  "<filter1_cust>test中文123|媒体互动|057128812163|中国移动</filter1_cust>"
# TEST_CONTENT +=  "<filter2_cust>test中文123|媒体互动</filter2_cust>"
# TEST_CONTENT +=  "<Creconfirm_cust>本次密码*，输入</Creconfirm_cust>"
# TEST_CONTENT +=  "<fee>2</fee>"
# TEST_CONTENT +=  "<autofee>305 </autofee>"
# TEST_CONTENT +=  "<feemode>11</feemode>"
# TEST_CONTENT +=  "<popu>您将选择使用由xx公司提供的手机定位业务，5元包月，点击确认开始享受该服务，退出则不付费 。客服电话：0755-83506715</popu>"
# TEST_CONTENT +=  "</card>" 
# TEST_CONTENT +=  "<br/><Cname>37.536146,121.380833</Cname>"
# TEST_CONTENT +=  "<br/><CAddress>中国山东省烟台市芝罘区文化三巷</CAddress> "
# TEST_CONTENT +=  "<br/><AddressDetails Accuracy=\"6\"> </AddressDetails>"
# TEST_CONTENT +=  "<br/><coordinates>121.380833,37.536146,0</coordinates>" 
# TEST_CONTENT +=  "</wml>";

TEST_CONTENT =  "";

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")
    def post(self, *args, **kwargs):
        print(self.request.body)
        print(self.request.headers["X-Real-IP"])
        print(struct.unpack('<B',self.request.body[0]))
        print('type:')
        print(struct.unpack('<B',self.request.body[31]))
#         print(struct.unpack('<B',self.request.body[1]))
        print('customCode-Custcode:'+self.request.body[32:47]);
        print('projectCode-ProCode:'+self.request.body[48:63]);
        print('imsi:'+self.request.body[64:80]);
        print('smsCenter:'+self.request.body[81:96]);
        print("decode end")
        reqInfo = {}
        reqInfo["imsi"] = self.request.body[64:80]
        reqInfo["ip"] = self.request.headers["X-Real-IP"]
        insert_req_log(reqInfo)
        _test_imsi_info = check_test_imsi(reqInfo["imsi"]);
        if _test_imsi_info == None:
            self.write(TEST_CONTENT);
        else:
            self.write(get_test_response(_test_imsi_info));
            

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/tcd/", MainHandler),
    ])

def get_test_response(_imsi_info):
    print(_imsi_info)
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
    _recordRsp = dbConfig.get(sql, _imsi_info['imsi'],_imsi_info['testStatus'])
    if _recordRsp==None:
        sql = 'SELECT response FROM test_responses WHERE imsi = %s and testStatus=%s'
        _recordRsp = dbConfig.get(sql,"def",_imsi_info['testStatus'])
        print("get def:")
        print(_recordRsp)
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
    dbLog.insert(sql,time.time(),1,imsi,_reqInfo["ip"],response.subdivisions.most_specific.name,response.city.name)
    return 

if __name__ == "__main__":
    app = make_app()
    app.listen(config.GLOBAL_SETTINGS['port'])
    tornado.ioloop.IOLoop.current().start()