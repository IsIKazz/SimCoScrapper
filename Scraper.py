'''
Created on Oct 19, 2017

@author: FB
'''

from selenium import webdriver
import time
import numpy as np
import os
import shutil
import numpy.matlib
import datetime
from datetime import timedelta
import urllib.request
import pandas as pd
import sys
from statsmodels.sandbox.distributions.sppatch import expect
pd.options.display.encoding = sys.stdout.encoding
from pandas.io.pytables import HDFStore
from subprocess import call
import math


class Scraper(object):
    '''
    classdocs
    Download csv-file of filled patents of day xxxx-xx-xx from google patents
    output = One csv-file (per day)
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.cwd = os.getcwd()
        
    def downloadCSV(self, startdate, waiting_time, download_dir, DaysTillStore):
        chrome_options = webdriver.ChromeOptions()
        preferences = {"download.default_directory": download_dir ,
                              "directory_upgrade": True,
                              "safebrowsing.enabled": True,
                              "extensions_to_open": "" }
        chrome_options.add_experimental_option("prefs", preferences)
        driver = webdriver.Chrome(chrome_options=chrome_options)
        patentfile = self.cwd + '\\PatentScraper\\patents.h5'
        Patent_info = pd.DataFrame(data=None, columns=["id", "title", "assignee", "inventor/author", "priority date", "filing/creation date", 
                         "publication date", "grant date", "result link"])
        
        for i in range(100000):
            time_delta_before = datetime.timedelta(days=i)
            time_delta_after = datetime.timedelta(days=i+1)
            Date_before = startdate-time_delta_before
            Date_before = Date_before.strftime("%Y%m%d")
            Date_after = startdate-time_delta_after
            Date_after = Date_after.strftime("%Y%m%d")
            
            # Das lass ich so drin, weil UNSERE Ladegeschwindigkeit nicht der einschränkende Faktor ist
            'check internet connection; only continue if connection is given'
            connection = 0
            while connection < 1:
                try:
                    urllib.request.urlopen('http://www.python.org/')
                    #return True
                    connection = 1
                except:
                    connection = 0
                    time.sleep(3)
                    print('no connection')
                    continue
            driver_connection = 0
            'check if webdriver is still running; only continue if functionality is given'
            while driver_connection <1:
                try:
                    driver.get('file:///C:/')
                    driver_connection = 1
                except:
                    driver_connection = 0
                    time.sleep(3)
                    print('driver crashed')
                    driver = webdriver.Chrome()
                    continue    
                  
            url = 'https://patents.google.com/xhr/query?url=before%3Dfiling%3A'+str(Date_before)+'%26after%3Dfiling%3A'+str(Date_after)+'&exp=&download=true'
            print(i, Date_before)
            driver.get(url)
            
            randNR = 1 + numpy.matlib.rand(1,1)*waiting_time
            time.sleep(randNR)
          
            filename = max([download_dir +"\\"+f for f in os.listdir(download_dir)], key=os.path.getctime)
            shutil.move(os.path.join(download_dir, filename),str(Date_before)+"_filing.csv")
            
            Data = pd.read_csv(str(Date_before)+"_filing.csv", skiprows=(1),header=(0))
            id = pd.DataFrame(Data, columns=["id"])
            title = pd.DataFrame(Data, columns=["title"])
            assignee = pd.DataFrame(Data, columns=["assignee"])
            inventor_author = pd.DataFrame(Data, columns=["inventor/author"])
            priority_date = pd.DataFrame(Data, columns=["priority date"])
            filing_creation_date = pd.DataFrame(Data, columns=["filing/creation date"])
            publication_date = pd.DataFrame(Data, columns=["publication date"])
            grant_date = pd.DataFrame(Data, columns=["grant date"])
            result_link = pd.DataFrame(Data, columns=["result link"])
            Current_Patent_Content = np.concatenate((id, title, assignee, inventor_author, priority_date, filing_creation_date, publication_date, grant_date,
                                                     result_link), axis=1)
            df = pd.DataFrame(Current_Patent_Content, columns=["id", "title", "assignee", "inventor/author", "priority date", "filing/creation date", 
                                                     "publication date", "grant date", "result link"])

            Patent_info = Patent_info.append(df)
            
            if i == math.trunc(i/DaysTillStore)*DaysTillStore:
            
                store = HDFStore(patentfile, complevel=4)
    
                Patent_info = Patent_info[["id", "title", "assignee", "inventor/author", "priority date", "filing/creation date", 
                                             "publication date", "grant date", "result link"]]
        
                print("cumm", Patent_info.shape)
            
                try:
                    Patent_info_store = store['Patent_info']
                    Patent_info_store = Patent_info_store.append(Patent_info)
                except KeyError:
                    Patent_info_store = Patent_info                
                
                    
                print(Patent_info_store.shape)
                
                store['Patent_info'] = Patent_info_store        
                
                # compress file... otherwise it will by 100ds of GB large - Compressed already in store without command file
                
#                 store.close()
#                 outfilename =  self.cwd +'\\PatentScraper\\out.h5'
#                 command = ["ptrepack", "-o", "--chunkshape=auto", "--propindexes", patentfile, outfilename]
#                 print('Size of %s is %.2fMB' % (patentfile, float(os.stat(patentfile).st_size)/1024**2))
#                 if call(command) != 0:
#                     print('Error')
#                 else:
#                     print('Size of %s is %.2fMB' % (outfilename, float(os.stat(outfilename).st_size)/1024**2))
#                 os.remove(patentfile)
#                 os.renames(outfilename, patentfile)
                store.close()
                print('Size of %s is %.2fMB' % (patentfile, float(os.stat(patentfile).st_size)/1024**2))
                # Reset DataFrame
                Patent_info = pd.DataFrame(data=None, columns=["id", "title", "assignee", "inventor/author", "priority date", "filing/creation date", 
                                         "publication date", "grant date", "result link"])
            
        print("done")

        driver.close()
        

            
