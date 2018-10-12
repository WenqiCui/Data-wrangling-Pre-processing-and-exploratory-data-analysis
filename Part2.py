import urllib.request
import logging
import zipfile
import os
import pandas as pd
import shutil #to delete the directory contents
import glob
import sys
import time
import datetime

#Initializing logging file #
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch1 = logging.FileHandler('problem2_log.log') #output the logs to a file
ch1.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch1.setFormatter(formatter)
root.addHandler(ch1)

ch = logging.StreamHandler(sys.stdout ) #print the logs in console as well
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)


# Cleanup required directories #
try:
    if not os.path.exists('downloaded_zips'):
        os.makedirs('downloaded_zips', mode=0o777)
    else:
        shutil.rmtree(os.path.join(os.path.dirname(__file__),'downloaded_zips'), ignore_errors=False)
        os.makedirs('downloaded_zips', mode=0o777)
    
    if not os.path.exists('downloaded_zips_unzipped'):
        os.makedirs('downloaded_zips_unzipped', mode=0o777)
    else:
        shutil.rmtree(os.path.join(os.path.dirname(__file__), 'downloaded_zips_unzipped'), ignore_errors=False)
        os.makedirs('downloaded_zips_unzipped', mode=0o777)
    logging.info('Directories cleanup complete.')
except Exception as e:
    logging.error(str(e))
    exit()       
    
# Function to Download zips #
def download_zip(url):
    zips = []
    try:
        zips.append(urllib.request.urlretrieve(url, filename= 'downloaded_zips/'+url[-15:]))
        if os.path.getsize('downloaded_zips/'+url[-15:]) <= 4515: #catching empty file
            os.remove('downloaded_zips/'+url[-15:])
            logging.warning('Log file %s is empty. Attempting to download for next date.', url[-15:])
            return False
        else:
            logging.info('Log file %s successfully downloaded', url[-15:])
            return True
    except Exception as e: #Catching file not found
        logging.warning('Log %s not found...Skipping ahead!', url[-15:])
        return True

# Fetch all the command line arguments #
argLen=len(sys.argv)
year=''
accessKey=''
secretAccessKey=''
inputLocation=''

for i in range(1,argLen):
    val=sys.argv[i]
    if val.startswith('year='):
        pos=val.index("=")
        year=val[pos+1:len(val)]
        continue
    elif val.startswith('accessKey='):
        pos=val.index("=")
        accessKey=val[pos+1:len(val)]
        continue
    elif val.startswith('secretKey='):
        pos=val.index("=")
        secretAccessKey=val[pos+1:len(val)]
        continue
    elif val.startswith('location='):
        pos=val.index("=")
        inputLocation=val[pos+1:len(val)]
        continue

print("Year=",year)
print("Access Key=",accessKey)
print("Secret Access Key=",secretAccessKey)
print("Location=",inputLocation)

# Generate URLs and download zip for the inputted year #

url_pre = "http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"
qtr_months = {'Qtr1':['01','02','03'], 'Qtr2':['04','05','06'], 'Qtr3':['07','08','09'], 'Qtr4':['10','11','12']}
valid_years = range(2010,2018)
days = range(1,32)

if not year:
    year = 2010
    logging.warning('Program running for 2010 by default since you did not enter any Year.')



url_final = []
for key, val in qtr_months.items():
    for v in val:
        for d in days:
            url = url_pre +str(year) +'/' +str(key) +'/' +'log' +str(year) +str(v) + str(format(d,'02d')) +'.zip'
            if download_zip(url):
                break
            else:
                continue


# Unzip the logs and extract csv #
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


# Load the csvs into dataframe #
try:
    filelists = glob.glob('downloaded_zips_unzipped' + "/*.csv")
    all_csv_df_dict = {period: pd.read_csv(period) for period in filelists}
except Exception as e:
    logging.error(str(e))
    exit()
                                     

try:
    for key, val in all_csv_df_dict.items():
        df = all_csv_df_dict[key]
        #detecting null values
        null_count = df.isnull().sum()
        
        # variable idx should be either 0 or 1
        incorrect_idx = (~df['idx'].isin([0.0,1.0])).sum()
        
        # variable norefer should be either 0 or 1
        incorrect_norefer = (~df['norefer'].isin([0.0,1.0])).sum() 
        
        # variable noagent should be either 0 or 1
        incorrect_noagent = (~df['noagent'].isin([0.0,1.0])).sum()
        
        #remove rows which have no ip, date, time, cik or accession
        df.dropna(subset=['cik'])
        df.dropna(subset=['accession'])
        df.dropna(subset=['ip'])
        df.dropna(subset=['date'])
        df.dropna(subset=['time'])
        
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
    
except Exception as e:
    logging.error(str(e))
    exit()
    
# Combining all dataframe and computing overall summary metric and writing csv #
try:
    master_df = pd.concat(all_csv_df_dict)
    master_df.to_csv('master_csv.csv')

except Exception as e:
    logging.error(str(e))
    exit()
    


