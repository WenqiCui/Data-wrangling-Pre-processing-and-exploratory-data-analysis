# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 15:09:15 2018

@author: wenqi
"""
import urllib
import csv
import os
import shutil
import logging
import time
from urllib import request
logging.basicConfig(filename='logger.log', level=logging.INFO)

table_count = 1;
def printtable(table,count):
    doc_name="table_"+str(count)+".csv";
    logging.info(str(time.asctime( time.localtime(time.time()) ))+"  Create new Table:"+doc_name)
    '''
    headers = [th.text for th in table.select("tr th")]
    with open(doc_name, "w") as f:
        wr = csv.writer(f)
        wr.writerow(headers)
        wr.writerows([[td.text.encode("utf-8") for td in row.find_all("td")] for row in table.select("tr + tr")])
    '''
    tab_data = [[item.text.encode("utf-8") for item in row_data.select("th,td")]
                for row_data in table.select("tr")]
    outfile = open(doc_name,"w",newline='',encoding='utf-8')
    writer = csv.writer(outfile)
    for data in tab_data:
  
        for i in range(len(data)):
            data[i] = str(data[i]).replace(u'\\xc2\\xa0', u' ')
            data[i] = str(data[i]).replace(u'\\n', u' ')
            data[i] = str(data[i]).replace('b\'', u' ')
            data[i] = str(data[i]).replace('\'', u' ')
        writer.writerow(data)

def geturl(acc):
    accn = acc[:10]+"-"+acc[10:12]+"-"+acc[12:]

    return accn



from configparser import ConfigParser
cfg = ConfigParser()
cfg.read('company.config')
cik = cfg.getint('parameter','cik')
acc = cfg.get('parameter','acc')

url = "https://www.sec.gov/Archives/edgar/data/"+str(cik)+"/"+ str(acc)+"/"+geturl(acc)+"-index.html"

try:
    response = urllib.request.urlopen(url)
except IOError:
    print("Invalid cik or acc")
content = response.read()
#print(content)

from bs4 import BeautifulSoup

soup = BeautifulSoup(content,'lxml')
all_href = soup.find_all('a')
flag = False;
for href in all_href:
    if (("10q" in href['href'])or("10-q" in href['href'])):
        flag = True
        link_10q = "https://www.sec.gov"+href['href'];
if flag:
    content_10q = urllib.request.urlopen(link_10q).read()
else:
    print("Invalid cik or acc")
soup = BeautifulSoup(content_10q,'lxml')
table_div = soup.find_all("table",style="border:none;border-collapse:collapse;width:100%;",)
if (not os.path.exists("tables")):
    os.mkdir("tables")
    logging.info(str(time.asctime( time.localtime(time.time()) ))+"Make Direction: tables")
os.chdir("tables")
for div in table_div:
    printtable(div,table_count)
    table_count += 1
os.chdir("..")
shutil.make_archive("data","zip","tables")
logging.info(str(time.asctime( time.localtime(time.time()) ))+"Make archive")

'''
import boto3
s3 = boto3.resource('s3')
s3.meta.client.upload_file('data.zip','wenqi.7390','data.zip')
'''
import boto
import boto.s3
from boto.s3.key import Key
from boto.exception import S3ResponseError

#Need your own aws key
cfg.read('aws.config')
AWS_ACCESS_KEY_ID = cfg.get('parameter','AWSAccessKeyId')
AWS_SECRET_ACCESS_KEY = cfg.get('parameter','AWSSecretKey')


bucket_name = "7390wenqi"
try:
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY)

    bucket = conn.get_bucket(bucket_name)
except S3ResponseError:
    print("Invalid AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY")
k = Key(bucket)
k.key = str(cik)+"data.zip"
k.set_contents_from_filename("data.zip")
k.key =  str(cik)+"logger.log"
k.set_contents_from_filename("logger.log")





        