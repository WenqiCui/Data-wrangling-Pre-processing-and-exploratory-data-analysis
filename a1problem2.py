# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 22:06:26 2018

@author: wenqi
"""
import logging 
import shutil 
import glob
import boto.s3
import urllib.request
import zipfile
import os
import pandas as pd
import sys
from boto.s3.key import Key
import time
import datetime
import boto
from boto.exception import S3ResponseError

root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch1 = logging.FileHandler('problem2_log.log') #output the logs to a file
ch1.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch1.setFormatter(formatter)
root.addHandler(ch1)


def download_zip(url):
    zips = []
    zips.append(urllib.request.urlretrieve(url, filename= 'downloaded_zips/'+url[-15:]))

year='2010';
urll = "http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"
qtr_months = {'Qtr1':['01','02','03'], 'Qtr2':['04','05','06'], 'Qtr3':['07','08','09'], 'Qtr4':['10','11','12']}
if not os.path.exists('downloaded_zips'):
        os.makedirs('downloaded_zips')
else:
        shutil.rmtree(os.path.join(os.path.dirname(__file__),'downloaded_zips'), ignore_errors=False)
        os.makedirs('downloaded_zips')
    
if not os.path.exists('downloaded_zips_unzipped'):
        os.makedirs('downloaded_zips_unzipped')
else:
        shutil.rmtree(os.path.join(os.path.dirname(__file__), 'downloaded_zips_unzipped'), ignore_errors=False)
        os.makedirs('downloaded_zips_unzipped')
        
for key, val in qtr_months.items():
    for v in val:
            url = urll +str(year) +'/' +str(key) +'/' +'log' +str(year) +str(v) + '01' +'.zip'
            download_zip(url)
logging.info('Download files')    
        
try:
    zip_files = os.listdir('downloaded_zips')
    for f in zip_files:
        z = zipfile.ZipFile(os.path.join('downloaded_zips', f), 'r')
        for file in z.namelist():
            if file.endswith('.csv'):
                z.extract(file, r'downloaded_zips_unzipped')
except Exception as e:
        logging.error(str(e))
        exit()
logging.info('Extracted files')         
        
filelists = glob.glob('downloaded_zips_unzipped' + "/*.csv")
all_csv = {file: pd.read_csv(file) for file in filelists}
 

for key, val in all_csv.items():
        df = all_csv[key]  
        null_count = df.isnull().sum()
        logging.info('Count of Null values for %s in all the variables:\n%s ', key, null_count)
        
        # variable idx should be either 0 or 1
        incorrect_idx = (~df['idx'].isin([0.0,1.0])).sum()
        logging.info('There are %s idx which are not 0 or 1 in the log file %s', incorrect_idx, key) 
        
        # variable norefer should be either 0 or 1
        incorrect_norefer = (~df['norefer'].isin([0.0,1.0])).sum()
        logging.info('There are %s norefer which are not 0 or 1 in the log file %s', incorrect_norefer, key) 
        
        # variable noagent should be either 0 or 1
        incorrect_noagent = (~df['noagent'].isin([0.0,1.0])).sum()
        logging.info('There are %s noagent which are not 0 or 1 in the log file %s', incorrect_noagent, key) 
                
        #remove rows which have no ip, date, time, cik or accession
        df.dropna(subset=['cik'])
        df.dropna(subset=['accession'])
        df.dropna(subset=['ip'])
        df.dropna(subset=['date'])
        df.dropna(subset=['time'])   
        logging.info('Rows removed where ip, date, time, cik or accession were null.')        
        #replace nan with the most used browser in data.
        max_browser = pd.DataFrame(df.groupby('browser').size().rename('cnt')).idxmax()[0]
        df['browser'] = df['browser'].fillna(max_browser)
        
        # replace nan idx with max idx
        max_idx = pd.DataFrame(df.groupby('idx').size().rename('cnt')).idxmax()[0]
        df['idx'] = df['idx'].fillna(max_idx)
        
        # replace nan code with max code
        max_code = pd.DataFrame(df.groupby('code').size().rename('cnt')).idxmax()[0]
        df['code'] = df['code'].fillna(max_code)
        
        # replace nan norefer with zero
        df['norefer'] = df['norefer'].fillna('1')
        
        # replace nan noagent with zero
        df['noagent'] = df['noagent'].fillna('1')
        
        # replace nan find with max find
        max_find = pd.DataFrame(df.groupby('find').size().rename('cnt')).idxmax()[0]
        df['find'] = df['find'].fillna(max_find)
        
        # replace nan crawler with zero
        df['crawler'] = df['crawler'].fillna('0')
        
        # replace nan extention with max extention
        max_extention = pd.DataFrame(df.groupby('extention').size().rename('cnt')).idxmax()[0]
        df['extention'] = df['extention'].fillna(max_extention)
        
        # replace nan extention with max extention
        max_zone = pd.DataFrame(df.groupby('zone').size().rename('cnt')).idxmax()[0]
        df['zone'] = df['zone'].fillna(max_zone)
    
        # find mean of the size and replace null values with the mean
        df['size'] = df['size'].fillna(df['size'].mean(axis=0))
        
        ##### Summary Metrics #####
        #Compute mean size
        df['size_mean'] = df['size'].mean(axis=0)
        
        #Compute maximum used browser
        df['max_browser'] = pd.DataFrame(df.groupby('browser').size().rename('cnt')).idxmax()[0]
        
        #Compute distinct count of ip per month i.e. per log file
        df['ip_count'] = df['ip'].nunique()
        
        #Compute distinct count of cik per month i.e. per log file
        df['cik_count'] = df['cik'].nunique()
        logging.info('Handle missing data')
 
onedf = pd.concat(all_csv)
onedf.to_csv('onedf.csv')
logging.info('Compile all the data and summaries of the 12 files into one file:onedf.csv')
from configparser import ConfigParser
cfg = ConfigParser()
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
k.key = "onedf.csv"
k.set_contents_from_filename("onedf.csv")
k.key = "problem2_log.log"
k.set_contents_from_filename("problem2_log.log")
