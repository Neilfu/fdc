#!/usr/bin/env python

import time
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient


preUrl='http://esf.gz.soufun.com/housing/__0_3_0_0_'
suffUrl='_0_0/'

#初始化mongodb数据库
connection = MongoClient()
db=connection['fdcdb']
collection=db['xiaoqu']


#获取小区的地址位置坐标
def getLocation(url):
    res=s.get(url)
    str=re.findall(r'mapInfo=\{(.*?)\}',res.text)[0]
    px=float(re.findall('px:\"(.*?)\"',str)[0])
    py=float(re.findall('py:\"(.*?)\"',str)[0])
    return {'px':px,'py':py}

min=1
max= 101

s=requests.Session()
startT = time.time()
cnt=0;
for page in range(min,max):
    startP=time.time()
    url=preUrl+str(page)+suffUrl
    print url
    res=s.get(url)
    soup=BeautifulSoup(res.text)
    lis=soup.find(id='houselist').find_all('li')
    for li in lis:
        mapXiaoqu={}
        soufunUrl=li.find('dt').find('a').attrs['href'][0]
        name = li.find('dt').find('a').text
        kind = re.findall('(\w+)\.jpg',li.dt.img.attrs['src'])[0]
        address = li.dl.dt.span.nextSibling.encode('utf8')
        domain =  re.sub('[\[\]]','',li.dl.dt.span.text)
        mapUrl = li.find('a',class_="iconmap")['href']
        soufunId =re.findall('newcode=(\d+)',mapUrl)[0]
        price=0
        if li.find('div',class_="price"):
           price=float(li.find('div',class_="price").find('span').text)

        mapXiaoqu['名称']=name
        mapXiaoqu['类别']=kind      
        mapXiaoqu['地址']=address
        mapXiaoqu['编号']=soufunId
        mapXiaoqu['价格']=price
        mapXiaoqu['位置']=getLocation(mapUrl)
        mapXiaoqu['方位']=domain        
        '''
        for key in mapXiaoqu:
            print key.decode('utf8'), ":" ,mapXiaoqu[key]
        '''
        
        collection.insert(mapXiaoqu)
        cnt += 1
        endP = time.time()
        now=time.ctime()    
        print "%s:finish getting Page:%d,NO. %d xiaoqu:%s,cost time:%d, total time:%d" %(now,page,cnt,soufunId,endP-startP,endP-startT)
        
        
