# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 15:09:15 2018

@author: wenqi
"""
import urllib
import csv
import unicodedata
table_count = 1;
def printtable(table,count):
    doc_name="table_"+str(count)+".csv";
    print(doc_name)
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
            data[i] = unicodedata.normalize("NFKD", str(data[i]))
            
        writer.writerow(data)


def geturl(acc):
    tem = acc.split("-")
    accs = ""
    for i in [0,1,2] :
        accs = accs + tem[i]
    accn = int(accs)
    return accn

cik = 51143
acc = "0000051143-13-000007"
url = "https://www.sec.gov/Archives/edgar/data/"+str(cik)+"/"+ str(geturl(acc))+"/"+acc+"-index.html"

response = urllib.request.urlopen(url)
content = response.read()
#print(content)

from bs4 import BeautifulSoup

soup = BeautifulSoup(content,'lxml')
all_href = soup.find_all('a')

for href in all_href:
    if ("10q" in href['href']):
        link_10q = "https://www.sec.gov"+href['href'];
content_10q = urllib.request.urlopen(link_10q).read()

soup = BeautifulSoup(content_10q,'lxml')
table_div = soup.find_all("table",style="border:none;border-collapse:collapse;width:100%;",)
for div in table_div:
    printtable(div,table_count)
    table_count += 1


        