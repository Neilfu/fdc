#!/usr/bin/env python

import time
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

#初始化mongodb数据库
connection = MongoClient()
db=connection['fdcdb']
collection=db['gztable']
key5='总层数'
key4=u'总层数'
key3='盘源编号'
cur=collection.remove({'发布日期':'2013-05-12'})


for row in cur:
    for key in row:
        print key,row[key]
    

