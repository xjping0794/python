#!/usr/bin/env pypy
# -*-coding:utf-8 -*-
# Script Name           : signal_data_check.py
# Author                    : xjping0794
# Created                   : 9 May 2017
# Last Modified         :
# Version                   : 2.6
# Modifications     :
# Description       : check file content including field num verification,field length checksum,number verification,date verification
import ConfigParser
import os,sys,socket
import re,logging,time,datetime
from ftplib import FTP

CONFIG_FILE_NAME="/data2/http/bin/signal_field.cfg"
statsmin="201705172114"
endmin="202001012359"

def filewarnlog(msg):
    ISOTIMEFORMAT='%Y-%m-%d %X'
    curstdtime=time.strftime(ISOTIMEFORMAT, time.localtime())
    f = open('/data2/http/log/warningdata.log', 'ab+')  # open for 'w'riting
    f.write(str(curstdtime)+"\t"+":"+msg+"\n")  # write text to file
    f.close()  # close the file
    print msg
# def logging.info(msg):
#     logging.basicConfig(level=logging.DEBUG,
#                          format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
#                          datefmt='%a, %Y %m %d %H:%M:%S',
#                          filename='/data2/http/log/signal_data_check.log',
#                          filemode='a')
#     logging.info(msg)

class load_config():
    """加载信令配置文件"""

    def load_signal_map(self):
        try:
            mappings = ConfigParser.ConfigParser()
            mappings.read(CONFIG_FILE_NAME)
            secs=mappings.sections()
            for sec in secs:
                return self.Forwardcomment(dict(mappings.items(sec)))
        except ConfigParser.NoSectionError:
            return {}
    def get_config_value(self,config_map,option):
        config_value=config_map.get(option)
        return config_value if config_value is not None else "config is not exists"
    def get_len_option(self,config_map):
        checklenfield = []
        for key in config_map.keys():
            if key.endswith('len'):
                checklenfield.append(key)
                print key
        return checklenfield
    def Forwardcomment(self,config_map):
        for key,value in config_map.items():
            config_map[key]=re.sub(r'//.*','',value).rstrip()
        return config_map
class data_check():
    """校验数据内容"""
    def check_field_num(self,cffieldnum,filefieldnum):
        return 1 if int(cffieldnum) == int(filefieldnum) else 0
    def check_field_length(self,cffieldlen,filefieldlen):
        return 1 if int(cffieldlen) >= int(filefieldlen) else 0
    def check_msisdn_valid(self,data_file_name,lineno,msisdn):
        if msisdn.strip() == '':
            filewarnlog("数据文件名： "+data_file_name+" 行号："+str(lineno)+" 中电话号码字段为空")
            return 0
        if not msisdn.isdigit ():
            filewarnlog("数据文件名： "+data_file_name+" 行号："+str(lineno)+" 中电话号码字段："+msisdn+" 不全为数字")
            return 0
        if msisdn.startswith("86") and len(msisdn)<>13:
            filewarnlog("数据文件名： "+data_file_name+" 行号："+str(lineno)+" 中86开头电话号码："+msisdn+" 长度不为13位")
            return 0
        elif not msisdn.startswith("86") and not msisdn.startswith("1"):
            filewarnlog ("数据文件名： " + data_file_name + " 行号：" + str (lineno) + " 中非1开头电话号码：" + msisdn)
            return 0
        if msisdn.startswith("1") and len(msisdn)<>11:
            filewarnlog("数据文件名： "+data_file_name+" 行号："+str(lineno)+" 中1开头电话号码："+msisdn+" 长度不为11位")
            return 0
        return 1
    def check_time_valid(self,data_file_name,lineno,start_time,end_time,file_time):
        if start_time>end_time:
            filewarnlog("数据文件名： "+data_file_name+" 行号："+str(lineno)+" 开始时间字段"+start_time+" 大于结束时间字段:"+end_time)
        start_time=datetime.datetime.fromtimestamp(int(start_time))
        file_time=transtostdtime(file_time)
        minustime(data_file_name, lineno, start_time, file_time)
    def check_flow(self,data_file_name,lineno,upflow,downflow):
        try:
            if int(upflow) >= 5368709120:
                filewarnlog ("数据文件名： " + data_file_name + " 行号：" + str (lineno) + " 中上行流量：" + str(upflow) + " 大于5G")
            if int(downflow) >= 5368709120:
                filewarnlog ("数据文件名： " + data_file_name + " 行号：" + str (lineno) + " 中下行流量：" + str(downflow) + " 大于5G")
        except:
            filewarnlog ("数据文件名： " + data_file_name + " 行号：" + str (lineno) + " 中上行流量：" + str (upflow) + " 下行流量：" + str (downflow) + " 存在非数值内容")
    def check_lac_cell(self,data_file_name,lineno,lac,cell):
        if lac not in lacid:
            filewarnlog ("数据文件名： " + data_file_name + " 行号：" + str (lineno) + " 中基站lac：" + str (lac) + " 不在维表中")

        if cell not in cellid:
            filewarnlog("数据文件名： " + data_file_name + " 行号：" + str (lineno) + " 中基站cell：" + str (cell) + " 不在维表中")

def read_lac_cell_config():
    global lacid,cellid
    f = open (r"/data2/http/bin/td_np_cell", 'r')
    sourceInLines = f.readlines ()
    f.close ()
    lac = []
    cell = []
    for line in sourceInLines:
        lac.append (line.replace("\n","").split (',')[0])
        try:
            hexval=hex(int(line.replace("\n","").split (',')[1])).upper()
            if hexval.startswith('0X'):
                cell.append(hexval[2:])
        except ValueError:
            logging.error("维表中cell字段值:"+line.replace("\n","").split (',')[1]+" 异常,抛出并不参与比对")
    lacid=list(set(lac))
    cellid=list(set(cell))

def readini():
    global config_map,cffieldnum,cf_msisdn_row_position,cf_starttime_row_position,cf_endtime_row_position
    global lac_position,cell_position,upflow_position,downflow_position
    config_map=load_config().load_signal_map()
    cffieldnum=load_config().get_config_value(config_map,'fieldnum')
    cffield1loc,cffield1len=load_config().get_config_value(config_map,'field1len').split(",")
    cf_msisdn_row_position=load_config().get_config_value(config_map,'msisdn_row_position')
    cf_starttime_row_position=load_config().get_config_value(config_map,'starttime_row_position')
    cf_endtime_row_position=load_config().get_config_value(config_map,'endtime_row_position')
    lac_position = load_config().get_config_value(config_map,'lac_row_position')
    cell_position = load_config().get_config_value(config_map,'cell_row_position')
    upflow_position = load_config().get_config_value(config_map,'upflow_row_position')
    downflow_position = load_config().get_config_value(config_map,'downflow_row_position')

def minustime(data_file_name,lineno,date1,date2):
    if str(date1) > str(date2):
        maxdate=str(date1)
        date1=str(date2)
        date2=maxdate
    date1 = time.strptime (str(date1), "%Y-%m-%d %H:%M:%S")
    date2 = time.strptime (str(date2), "%Y-%m-%d %H:%M:%S")
    date1 = datetime.datetime (date1[0], date1[1], date1[2], date1[3], date1[4], date1[5])
    date2 = datetime.datetime (date2[0], date2[1], date2[2], date2[3], date2[4], date2[5])
    timedel=date2 - date1
    return parsetimedel(data_file_name,lineno,timedel)

def hextoint(paramvalue):
    try:
        return int(str(paramvalue),16)
    except:
        return False

def parsetimedel(data_file_name,lineno,timevalue):
    "解析时间差，偏差在4个小时以上即输出"
    matchwords="day"
    if re.search(matchwords,str(timevalue)):
        filewarnlog("数据文件名： "+data_file_name+" 行号："+str(lineno)+" 内容延时超过:"+str(timevalue))
        return False
    else:
        if int(str(timevalue).split(":")[0]) >=4:
            filewarnlog("数据文件名： "+data_file_name+" 行号："+str(lineno)+" 时间延时超过:"+str(timevalue))
            return False
    return True

def transtostdtime(commontime):
    "将 20161231120005 转换成 2016-12-31 12:00:05"
    return commontime[0:4]+"-"+commontime[4:6]+"-"+commontime[6:8]+" "+commontime[8:10]+":"+commontime[10:12]+":"+commontime[12:14]

def getminutesago(curtime,offset):
    curtime=curtime[0:4]+"-"+curtime[4:6]+"-"+curtime[6:8]+" "+curtime[8:10]+":"+curtime[10:12]
    print curtime
    curdate=datetime.datetime.strptime(str(curtime), "%Y-%m-%d %H:%M")
    return ((curdate-datetime.timedelta(minutes=offset)).strftime("%Y%m%d%H%M"))

def ftpdownloadfile(dealtime):
    global downloadfile,absfilename
    ftp = FTP()
    timeout = 30
    port = 21
    ftp.connect ('10.173.45.14', port, timeout)  # 连接FTP服务器
    ftp.login ('yaxin', 'yunnhc@YX')  # 登录
    ftp.cwd('miadata')  # 设置FTP路径
    list = ftp.nlst()  # 获得目录列表
    filterlist = []
    for name in list:
        if name.endswith('txt') and name.startswith('S1U-103-'+dealtime):
            filterlist.append (name)
    for downloadfile in filterlist:
        print downloadfile
    absfilename = '/data2/http/' + downloadfile  # 文件保存路径
    f = open(absfilename, 'wb')  # 打开要保存文件
    filename = 'RETR ' + downloadfile  # 保存FTP文件
    ftp.retrbinary(filename, f.write)  # 保存FTP上的文件
    ftp.quit()  # 退出FTP服务器
    logging.info("结束下载")

if __name__ == "__main__":
    logging.basicConfig (level=logging.DEBUG,
                         format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                         datefmt='%a, %Y %m %d %H:%M:%S',
                         filename='/data2/http/log/signal_data_check.log',
                         filemode='a')
    readini()
    read_lac_cell_config()
    print cellid[0:10]
    logging.info("读取配置文件："+"\n"+"cffieldnum:"+cffieldnum+"\n"+"cf_msisdn_row_position:"+cf_msisdn_row_position)
    """基于性能考虑，每次仅校验一个文件/每分钟"""
    while statsmin <= endmin:
        # downloadfile="S1U-103-20170517210500-047-11.txt"
        # absfilename = '/data2/http/' + downloadfile
        logging.info("开始时间为:"+statsmin+"的文件校验")
        logging.info("1、ftp下载文件")
        ftperrflag=1
        while ftperrflag<>0:
            try:
                ftpdownloadfile(statsmin)
                # 若文件不存在则等待1分钟后再扫描
                if re.search(statsmin,downloadfile):
                    ftperrflag=0
                else:
                    ftperrflag=1
                    time.sleep(5)
                    logging.warning(statsmin+" 点文件,数据源尚未准备好,等待1分钟后再次扫描")
            except (socket.error, socket.gaierror):
                logging.error("cannot reach 10.173.45.14")
                ftperrflag=1
                logging.warning("sleep 60 seconds to try again")
                time.sleep(60)

        logging.info("待校验文件名:"+absfilename)
        logging.info("2、开始校验文件数据：校验内容有字段数、字段长度、电话号码、时间、流量、基站。")

        """
        使用 with 结构,对可迭代对象 file，进行迭代遍历：for line in file，会自动地使用缓冲IO（buffered IO）以及内存管理，
        而不必担心任何大文件的问题。
        """
        datacheck=data_check()
        loadconfig=load_config()
        lineno=0
        try:
            retry=0
            with open(absfilename) as file:
                for line in file:
                    lineno += 1
                    # 校验字段数
                    if not datacheck.check_field_num(cffieldnum,len(line.split("|"))):
                        filewarnlog("数据文件名： "+downloadfile+" 行号："+str(lineno)+" 字段数不过, 数据字典中字段数为"+str(cffieldnum)+" 文件传过来字段数为"+str(len(line.split ("|"))))
                        continue
                    # 校验字段长度
                    # for field in loadconfig.get_len_option(config_map):
                    #     cffieldloc, cffieldlen=loadconfig.get_config_value(config_map,field).split(",")
                    #     filefieldlen=len((line.split(",")[cffieldloc].rstrip()))
                    #     if not datacheck.check_field_length(cffieldlen,filefieldlen):
                    #         filewarnlog("数据文件名： "+downloadfile+" 行号："+str(lineno)+" 字段:"+field[:-3]+" 数据字典中字段长度为"+cffieldlen+"文件传过来的字段长度为"+filefieldlen)
                    # 校验电话号码
                    datacheck.check_msisdn_valid(downloadfile,lineno,line.split("|")[int(cf_msisdn_row_position)].rstrip())
                    # 校验时间
                    starttime=line.split("|")[int(cf_starttime_row_position)][0:10].rstrip()
                    endtime=line.split("|")[int(cf_endtime_row_position)][0:10].rstrip()
                    filetime=re.search(r'S1U-\d{1,}-(\d{14})-\d{1,}-\d{1,}',downloadfile).group(1)
                    datacheck.check_time_valid(downloadfile,lineno,starttime,endtime,filetime)
                    # 校验流量
                    upflow=line.split("|")[int(upflow_position)].rstrip()
                    downflow=line.split("|")[int(downflow_position)].rstrip()
                    datacheck.check_flow(downloadfile,lineno,upflow,downflow)
                    # 校验基站
                    lac = line.split ("|")[int (lac_position)].rstrip ()
                    cell = line.split ("|")[int (cell_position)].rstrip()
                    datacheck.check_lac_cell(downloadfile,lineno,lac,cell)


        except IOError:
            logging.error(absfilename+"文件不存在")
            time.sleep(60)
            retry=1
        if retry == 1:
            logging.info("3、重新开始处理" + statsmin+"的文件校验")
            statsmin = getminutesago(statsmin, 0)
        else:
            logging.info("3、结束文件名校验:"+absfilename)
            time.sleep(5)
            statsmin=getminutesago(statsmin,-1)  #获取后一分钟时间