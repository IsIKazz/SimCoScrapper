import threading
import time
from selenium import webdriver

import os



import pandas as pd
import numpy as np
import sys
import urllib3
from lxml import html
import requests
from selenium.common.exceptions import NoSuchElementException
from pandas.io.pytables import HDFStore
from multiprocessing.pool import ThreadPool

chromeOptions = webdriver.ChromeOptions()
#prefs = {"profile.managed_default_content_settings.images":2}
#chromeOptions.add_experimental_option("prefs",prefs)
chromeOptions.add_argument("--headless")  


pd.options.display.encoding = sys.stdout.encoding
import urllib.request

exitFlag = 0

class myThread (threading.Thread):
    def __init__(self, threadID, name, counter, urlList):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.URLList = urlList
        self.name = name
        self.counter = counter
        print ("Initializing " + self.name)
        self.patenReader = patentReader(threadID, name)
    def run(self):
        print ("Starting " + self.name)
        self.patenReader.openURL(self.URLList,self.threadID, self.name)
        print ("Exiting " + self.name)
        
class patentReader():
    def __init__(self,threadID, name):
        self.driver = webdriver.Chrome(chrome_options=chromeOptions)

    def openURL(self,URL,threadID='1', threadName="Thread"):
        print("Scrapping Nr of URLS", URL.shape)
        
        if exitFlag:
            threadName.exit()
        
        Citing_Content = pd.DataFrame(data=None, columns=["Company", "Id", "Citing"])            
        for i in range (0, URL.size):
            #i = i+threadID
            print(i)
            start = time.time()
            try:
                self.driver.get(URL.iloc[i])
                currentContent = self.driver.find_element_by_css_selector("h3[id=citedBy] + div.responsive-table.style-scope.patent-result div.tbody").text                    
                currentContent = currentContent.split('\n')
                df = pd.DataFrame({"Company":Company.iloc[i],"Id":Id.iloc[i],"Citing":currentContent})
                Citing_Content = Citing_Content.append(df)
                end = time.time()
                print("file:", file, "threadID:", threadID, "row:", i, "time in sec:", round(end - start, 2), 'Cited by patents:', len(currentContent), 'URL:', URL.iloc[i])
                
            except NoSuchElementException:
                print(i, "time in sec:", round(time.time() - start, 2), 'Patent has not been cited.')
                pass
            
            except Exception as e: 
                print(e)
                end = time.time()
                print(i, "time in sec:", round(end - start, 2), 'exception on loading')
                pass
#                     for index, row in currentContent.iterrows():            
#                         row['Citing'] = row['Citing'].encode(sys.stdout.encoding, errors='replace')
#                         try:
#                             row['Citing'] = str(row['Citing'],'utf-8')
#                         except: 
#                             row['Citing'] = str(row['Citing'])
                    
        cwd = os.getcwd()
                
        writer = pd.ExcelWriter(cwd +'\\Citing_Content_'+str(file)+'_'+str(1)+'.xlsx', engine='xlsxwriter')
        Citing_Content.reset_index(inplace=True)
        Citing_Content.to_excel(writer, sheet_name='Sheet1')
        workbook  = writer.book
        workbook.close()        
                
            

if __name__ == '__main__':
    file = "Sample"
    
    'load patents from Company_patents:'
    patentfile = os.getcwd() + '\\patents.h5'
    nrThreads = 8
     
#     store = HDFStore(patentfile, complevel=4)
#     
#     Patent_info_store = store['Patent_info']
#     
#     
#     count_URLs = Patent_info_store.shape[0]
#     Company = Patent_info_store["assignee"]
#     Id = Patent_info_store["id"]
#     URL = Patent_info_store["result link"]
#     store.close()

    
    Data = pd.ExcelFile(file+'.xlsx')
    df = Data.parse('Sheet1')
    count_URLs = df.shape[0]
    Company = df.iloc[0:,2]
    Id = df.iloc[0:,0]
    URL = df.iloc[0:,8]
    print("Number of Patents to load: ", count_URLs)
    overall_start = time.time()
    
    #Split all URLs in equal chunks based on the number of expected threads
    urlLists = list()
    for g, df in URL.groupby(np.arange(len(URL))// (len(URL)/nrThreads)):
        print(df.shape)
        urlLists.append(df)
    
    # Create new threads
   
    
    allThreads = list()
    for i in range(0,nrThreads):
        allThreads.append(myThread(i, "Thread-" + str(i), i, urlLists[i]))
    
    # Start new Threads
    start = time.time()
    
    for thread in allThreads:
        thread.start()        
    for thread in allThreads:  
        thread.join()   
    
    
    end = time.time()
    print('time in h:', round(end - start, 2)/60/60)
    
    #TODO:Bugfixing: programm does not finish!