
import os
import sys
import threading
import time
import urllib


from pandas.io.pytables import HDFStore

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException,WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import json


import numpy as np
import pandas as pd
from selenium.webdriver.support.wait import WebDriverWait
import logging


chromeOptions = webdriver.ChromeOptions()
#prefs = {"profile.managed_default_content_settings.images":2}
#chromeOptions.add_experimental_option("prefs",prefs)
chromeOptions.add_argument("--headless")  


pd.options.display.encoding = sys.stdout.encoding

exitFlag = 0

class myThread (threading.Thread):
    def __init__(self, threadID, name, scrapList):
        super(myThread,self).__init__()
        #threading.Thread.__init__(self)
        self.threadID = threadID
        self.scrapList = scrapList
        self.name = name
        
        logging.debug("Initializing " + self.name)
        
    def run(self):
        logging.debug("Starting " + self.name)
        self._return = patentReader().openURL(self.scrapList,self.threadID, self.name)
        logging.debug("Exiting " + self.name)
    def join(self):
        threading.Thread.join(self)
        return self._return
        
class patentReader():
    def __init__(self):
        cwd = os.getcwd()        
        #System.setProperty("webdriver.chrome.driver", cwd+"//chromedriver.exe");
        

    def openURL(self,pd_Patents,threadID='1', threadName="Thread"):
        nrPatents = len(pd_Patents)
        pd_Patents.reset_index(inplace=True, drop=True)
        self.driver = webdriver.Chrome(chrome_options=chromeOptions)
        
        if exitFlag:
            threadName.exit()
        
        Citing_Content = pd.DataFrame()
        patent_HTML = pd.DataFrame()
                             
        for i, row in pd_Patents.iterrows():
            logging.info("Currently on row: {} / {}".format( i+1, nrPatents))
            #i = i+threadID
            
            start = time.time()
            try:
                self.driver.get(row["result link"])
                   
                try:
                    WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h3[id=legalEvents] + div.responsive-table.style-scope.patent-result div.tbody')))
                except TimeoutException:
                    logging.warn("no legal Event")
                    pass
                #currentHTML = self.driver.find_element_by_tag_name('body').get_attribute('innerHTML')  
                
                
                fp = urllib.request.urlopen(row["result link"])
                
                mybytes = fp.read()

                currentHTML = mybytes.decode("utf8")
                fp.close()
#                 tz=pd.read_html(currentHTML)
#                 
#                 cwd = os.getcwd()
#                 path = cwd + '\\tz.xlsx'
#                 writer = pd.ExcelWriter(path)
#                 tz.to_excel(writer, filename )
#                 writer.save()
                
                df2 = pd.DataFrame({"id":row["id"],"HTML":[currentHTML],"publication date":row["publication date"]})
                patent_HTML = patent_HTML.append(df2)
                
                jsonD = json.dumps(currentHTML)
                
                with open('my_filejson.html', 'w', encoding='utf-8') as fo:
                    fo.write(jsonD)
                    
                with open('my_filehtml.html', 'w', encoding='utf-8') as fo:
                    fo.write(currentHTML)
               
                Citing_Content = Citing_Content.append(df)
                
                logging.debug("threadID:", threadID,  "time in sec:", round(time.time() - start, 2), 'id:', row["id"], 'link', row["result link"])
                
            except WebDriverException as e:
                print(e)
                self.driver = webdriver.Chrome(chrome_options=chromeOptions)
                
            
            except Exception as e: 
                print(e)
                end = time.time()
                print(i, "time in sec:", round(end - start, 2), 'exception on loading')
                pass
        self.driver.quit()
        return patent_HTML
#                     for index, row in currentContent.iterrows():            
#                         row['Citing'] = row['Citing'].encode(sys.stdout.encoding, errors='replace')
#                         try:
#                             row['Citing'] = str(row['Citing'],'utf-8')
#                         except: 
#                             row['Citing'] = str(row['Citing'])
                    
#         cwd = os.getcwd()
#                 
#         writer = pd.ExcelWriter(cwd +'\\Citing_Content_'+str(file)+'_'+str(threadName)+'.xlsx', engine='xlsxwriter')
#         Citing_Content.reset_index(drop=True,inplace=True )
#         Citing_Content.to_excel(writer, sheet_name='Sheet1')
#         workbook  = writer.book
#         workbook.close()        
                
            

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )  
    file = "Sample"
    overall_start = time.time()
    'load patents from Company_patents:'
    
    cwd= os.getcwd()
    patentfile = cwd + '\\patents.h5'
    nrThreads = 1
     
    store = HDFStore(patentfile, complevel=4)
     
    patent_info_store = store['Patent_info']
    
    '''shortening PD for debugging'''
    patent_info_store = patent_info_store.iloc[0:100]

     
    # ['id', 'title', 'assignee', 'inventor/author', 'priority date', 'filing/creation date', 'publication date', 'grant date', 'result link']
    count_Patents = len(patent_info_store)
   
    store.close()
    
    
    
#     Data = pd.ExcelFile(file+'.xlsx')
#     df = Data.parse('Sheet1')
#     count_Patents = df.shape[0]
#     Company = df.iloc[:,2]
#     Id = df.iloc[:,0]
#     URL = df.iloc[:4,8]
    logging.info("Number of Patents to load: {}".format(patent_info_store.shape[0]))
    
    #Split the patents into a maximum number to save memory
    
    for __,patent_info in patent_info_store.groupby(np.arange(len(patent_info_store))//1000):    
        #Split all URLs in equal chunks based on the number of expected threads    
        
        urlLists = list()
        for g, df in patent_info.groupby(np.arange(len(patent_info))// (len(patent_info) /nrThreads)):
            logging.debug(df.shape)
            urlLists.append(df)
        
        # Create new threads
        allThreads = list()
        pdHTML = pd.DataFrame()
        
        for i in range(0,nrThreads):
            allThreads.append(myThread(i, "Thread-" + str(i), urlLists[i])) 
        
        for thread in allThreads:
            thread.start()  
        
        for thread in allThreads:  
            pdHTML = pdHTML.append(thread.join())   
        
        
        
        pdHTML = pdHTML.reset_index(drop=True)
        
        pdHTML["publication date"] = pd.to_datetime(pdHTML["publication date"])
        
        pdHTML.index = pdHTML["publication date"]
        
        
        
        for __, df in pdHTML.groupby(pd.TimeGrouper(freq='M')):
            
            if len(df.index)>0: 
        
                htmlfile = cwd + '\\htmlStore\\html' + "-" + str(df["publication date"].iloc[0].year) + "-" + str(df["publication date"].iloc[0].month) + '.h5'
                #
                
                #Appends the new records to the old records and drops duplicates 
                
                
                try:
                        with HDFStore(htmlfile, complevel=8) as store:
                            storePD = store['pdHTML']
                            storePD= storePD.append(df)
                            store.close()
                        os.remove(htmlfile)
                except KeyError:
                        storePD = df       
                 
                storePD = storePD.drop_duplicates(subset=['id'], keep='last')   
                storePD = storePD.reset_index(drop=True)
                with HDFStore(htmlfile, complevel=8) as store2:
                    store2['pdHTML'] = storePD
                    store2.close()
    
    end = time.time()
    
    runtime=round(end - overall_start, 2)/60/60
    
    hours, rem = divmod(time.time() - overall_start, 3600)
    minutes, seconds = divmod(rem, 60)
    logging.info('time in h: {:0>2}:{:0>2}:{:05.2f} for {} records with {} threads'.format(int(hours),int(minutes),seconds,len(patent_info_store),nrThreads))
    logging.info('Please Wait.... Shutting Threads Down')
 
    