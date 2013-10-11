#!/usr/bin/env python

import time
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient



#初始化mongodb数据库
connection = MongoClient()
db=connection['fdcdb']
xqCollection=db['xiaoqu']
houseCollection=db['gztable']

cur=xqCollection.find({})

total=0
mapXq={}
startT = time.time()
for row in cur:
    xqName=row[u'名称']
    xqLocation=row[u'位置']
    #print xqName, xqLocation
    pattern =re.compile(xqName)
    cur0= houseCollection.find({u'发布坐落':pattern})

    cnt= cur0.count()
    mapXq[xqName]= cnt
    total =total + cnt
    #print xqName,"found %d record" %(cur0.count())
    for r1 in cur0:
        r1[u'经度'] =xqLocation[u'px']
        r1[u'纬度'] =xqLocation[u'py']
        r1[u'小区']= xqName
        houseCollection.update({u"盘源编号":r1[u'盘源编号']},r1)
        
endP = time.time()
print "found %d record, cost time:%d" %(total, endP-startT)


