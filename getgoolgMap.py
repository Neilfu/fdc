#!/usr/bin/env python

import time
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

from geopy import geocoders
g = geocoders.GoogleV3()


#初始化mongodb数据库
connection = MongoClient()
db=connection['fdcdb']
houseCollection=db['gztable']

cur=houseCollection.find({})[62500:65000]

mapCount={"sucess":0,"fail":0,"error":0}

startT = time.time()
for row in cur:

    if (u'经度' in row) and  (row[u'经度'] > 0):
        print "skip %s" %(row[u'盘源编号'])
        continue
    
    addr,(lat,lng)=("",(-1,-1))
    district=row[u'区划']
    addr=row[u'发布坐落']
    result=re.search(district,addr)
    if result is None:
        addr=district+addr    
    try:
        addr,(lat,lng)=g.geocode(addr,language='zh-CN')
    except:
        mapCount['fail'] += 1
        

    row[u'谷歌地址']= addr
    row[u'经度']=lng
    row[u'纬度']=lat
    for f in row:
        if isinstance(row[f],(unicode)):
            row[f]=re.sub("\s","",row[f])
    try:    
        houseCollection.update({u"盘源编号":row[u'盘源编号']},row)
    except:
        print "error:%d, update:%s" %(mapCount['error'],row[u'盘源编号'])
        mapCount['error'] += 1
        continue
    endP = time.time()
    mapCount['sucess'] += 1
    now=time.ctime()
    print  "OK:%d addr:%s,cost time:%d" %(mapCount['sucess'],addr,endP-startT)
        

print "found %d record,fail:%d,error:%d,totoal:%d" %(mapCount['sucess'],mapCount['fail'],mapCount['error'],mapCount['sucess']+mapCount['fail']+mapCount['error'])


