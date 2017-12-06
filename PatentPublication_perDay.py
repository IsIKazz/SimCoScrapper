'''
Created on May 31, 2017

@author: FB
'''

'Loading Patent-Meta-Data from Groogle Patents'

' Load all patents that were filled on day xxxx-xx-xx'

from selenium import webdriver
import time
import numpy as np
import os
import shutil
import numpy.matlib
import datetime
from datetime import timedelta
import urllib.request

startdate = datetime.date(2010, 3, 2)
print(startdate)

download_dir = "C:\\Users\\FB\\simco\\KickML\\FirmScraper\\PatentScraper"
chrome_options = webdriver.ChromeOptions()
preferences = {"download.default_directory": download_dir ,
                      "directory_upgrade": True,
                      "safebrowsing.enabled": True,
                      "extensions_to_open": "" }
chrome_options.add_experimental_option("prefs", preferences)
driver = webdriver.Chrome(chrome_options=chrome_options)

for i in range(0, 10000):
    time_delta_before = datetime.timedelta(days=i)
    time_delta_after = datetime.timedelta(days=i+1)
    Date_before = startdate-time_delta_before
    Date_before = Date_before.strftime("%Y%m%d")
    Date_after = startdate-time_delta_after
    Date_after = Date_after.strftime("%Y%m%d")
    
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
    
    randNR = 1 + np.matlib.rand(1,1)*50
    time.sleep(randNR)
  
    filename = max([download_dir +"\\"+f for f in os.listdir(download_dir)], key=os.path.getctime)
    shutil.move(os.path.join(download_dir, filename),str(Date_before)+"_filing.csv")

print("done")
 