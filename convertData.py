#!/usr/bin/env python

import json
import time
import re
from pymongo import MongoClient

#初始化mongodb数据库
connection = MongoClient()
db=connection['fdcdb']
collection=db['gztable']

fileFdc=open("d:/fdc/fdcdb.txt","w")

titleList=(u'盘源编号',u'区划',u'发布坐落',u'放盘价',u'物业管理费',u'建筑面积',u'建筑时间',u'单价',u'套内建筑面积',u'总层数',u'户型',u'室',u'厅',u'卫',u'阳台',u'房屋朝向',u'装修程度',u'所在层数',u'使用性质',u'是否正在出租',u'是否带电梯',u'是否共有',u'交易状态',u'发布日期',u'小区',u'经度',u'纬度')  #,u'谷歌地址')

fileFdc.write("\t".join(titleList).encode("utf8")+"\n")
dictTitle={}
for i,title in enumerate(titleList):
    dictTitle[title]=i

myCur=collection.find()
for row in myCur:
    lists = [""] * len(titleList)
    for key in row:
        if key in titleList:           
            if isinstance(row[key],(unicode)):
                temp=re.sub("\s","",row[key])
                result=re.findall(u'(\d)室(\d)厅.*(\d)卫(\d)阳台',temp)
                if (len(result)>0):
                    lists[dictTitle[u'户型']] = temp 
                    lists[dictTitle[u'室']] = result[0][0]
                    lists[dictTitle[u'厅']] = result[0][1]
                    lists[dictTitle[u'卫']] = result[0][2]
                    lists[dictTitle[u'阳台']] = result[0][3]                      
                else:
                    lists[dictTitle[key]] =temp
            else:
                lists[dictTitle[key]] = str(row[key])
    if float(lists[dictTitle[u'建筑面积']]) <= 0:
        lists[dictTitle[u'建筑面积']] = lists[dictTitle[u'套内建筑面积']]

    try: 
        lists[dictTitle[u'单价']] =  unicode(float(lists[dictTitle[u'放盘价']]) / float(lists[dictTitle[u'建筑面积']]) *10000)
    except:
        lists[dictTitle[u'单价']] ="0"
    lists[dictTitle[u'物业管理费']]=re.sub('[^\d.]','',lists[dictTitle[u'物业管理费']])    
    lists[dictTitle[u'户型']]=""
    jStr=re.sub('\'','++',"\t".join(lists))
    fileFdc.write(jStr.encode("utf8")+"\n")             
fileFdc.close()
                  
    
        
