#!/usr/bin/env python

import json
import time
import re
from pymongo import MongoClient

#初始化mongodb数据库
connection = MongoClient()
db=connection['fdcdb']
collection=db['xiaoqu']

fileFdc=open("d:/fdc/location.txt","w")

title=(u'编号',u'名称',u'方位',u'地址',u'经度',u'纬度',u'类别',u'价格')
fileFdc.write('\t'.join(title).encode('utf8') + "\n")
dictTitle={}
for i,t in enumerate(title):
    dictTitle[t]=i
    
cur=collection.find({})
for row in cur:
    
    line = [""] * len(title)
    line[dictTitle[u'名称']] =  row[u'名称']
    line[dictTitle[u'地址']] =  row[u'地址']
    line[dictTitle[u'经度']] =  str(row[u'位置'][u'px'])
    line[dictTitle[u'纬度']] =  str(row[u'位置'][u'py'])
    line[dictTitle[u'编号']] =  row[u'编号']
    line[dictTitle[u'类别']] =  row[u'类别']
    line[dictTitle[u'价格']] =  str(row[u'价格'])
    line[dictTitle[u'方位']] =  row[u'方位']    
            
    fileFdc.write('\t'.join(line).encode('utf8') + "\n")

fileFdc.close()
