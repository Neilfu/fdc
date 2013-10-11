#!/usr/bin/env python

import time
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient


def getDetail(url):
    mapDetail={}
    res=s.get(url,headers=headers)
    soup1=BeautifulSoup(res.text)
    trs=soup1.find(id='con_one_1').find_all('tr')
    for tr in trs:
        tds=tr.find_all('td')
        if len(tds) != 4:
            continue
        #抽取有用字段及数值
        if tds[0].string and  tds[1].string:
            value =tds[1].string  #清除后缀"："
            #将字符串转化为浮点型（价格、面积字段）
            if value and re.match(ur'^\d+\.\d+$',value):
                value=float(value)
            name=re.sub(ur'：$','',tds[0].string)
            mapDetail[name] =  value

        if tds[2].string and  tds[3].string:
           value =tds[3].string  #清除后缀"："
            #将字符串转化为浮点型（价格、面积字段）
           if value and re.match(ur'^\d+\.\d+$',value):
               value=float(value)
           name=re.sub(ur'：$','',tds[2].string)
           mapDetail[name] = value
    return   mapDetail  

headers={"Host":" g4c.laho.gov.cn","Connection":" keep-alive","Content-Length":" 272","Cache-Control":" max-age=0","Accept":" text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Origin":" http://g4c.laho.gov.cn","User-Agent":" Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22","Content-Type":" application/x-www-form-urlencoded","Referer":" http://g4c.laho.gov.cn/search/clf/clfSearch.jsp","Accept-Encoding":" gzip,deflate,sdch","Accept-Language":" zh-CN,zh;q=0.8","Accept-Charset":" GBK,utf-8;q=0.7,*;q=0.3","Cookie":" JSESSIONID=9A4790B41728FF4292C294F4CD3D353D"}
url="http://g4c.laho.gov.cn/search/clf/clfSearch.jsp"
data={"pybh":"","xqmc":"","fbzl":"","jgfwStart":"-1","hxs":"-1","hxt":"-1","hxcf":"-1","hxw":"-1","hxyt":"-1","fwyt":"-1","jzmjStart":"-1","xzqh":"-1","jyzt":"-1","zjfwjgmc":"","fbrqStart":"","fbrqEnd":"","imgvalue":"","clfrandinput":"ce60ff163cab97029cc727e20e0fc3a7","orderfield":"","ordertype":"","chnlname":"(unable to decode value)","currPage":"1","judge":"1"}

#初始化mongodb数据库
connection = MongoClient()
db=connection['fdcdb']
collection=db['gztable']


min=0
max= 10

s=requests.Session()
startT = time.time()
cnt=0;
for page in range(min,max):
    startP=time.time()
    data["currPage"]=page
    #获得当前页
    r=s.post(url,data=data,headers=headers)
    
    #初始化成XPath对象
    soup = BeautifulSoup(r.text)
    #抽取数据字段名
    colTitle=[]
    ths=soup.find(id='tab').find_all('th')
    for th in ths:
        colTitle.append(th.string)
       
    #抽取数据行，和字段名意义对应，并保存在mapList字典中
    trs=soup.find(id='tab').find_all('tr')
    for tr in trs:
        
        tds=tr.find_all('td')
        if not tds:
            continue

        
        colValue=[]  
        for td in tds:

            tp =td.string
            #将字符串转化为浮点型（价格、面积字段）
            if tp and re.match(ur'^\d+\.\d+$',tp):
                tp=float(tp)
            colValue.append(tp)
        mapList={}
        for i,key in enumerate(colTitle):
            if i < len(colValue):
                mapList[key] = colValue[i]
        #detailUrl 是当前记录的详细描述页链接地址
        detailUrl = 'http://g4c.laho.gov.cn'+tds[1].find('a')['href']
        #houseId=tds[1].string
        print detailUrl
        mapList.update(getDetail(detailUrl))
        #mapList['_id']=long(houseId)
        #保存在数据库    
        collection.insert(mapList)
        cnt += 1
        endP = time.time()
        now=time.ctime()
        print "%s:finish getting Page:%d,NO. %d record:%s,cost time:%d, total time:%d" %(now,page,cnt,mapList[u'盘源编号'],endP-startP,endP-startT)

     
            
        
    
