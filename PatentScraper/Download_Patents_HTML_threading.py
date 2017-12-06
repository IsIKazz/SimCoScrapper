
import os
import sys
import threading
import time



from pandas.io.pytables import HDFStore

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException,\
    WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


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
        pass

    def openURL(self,pd_Patents,threadID='1', threadName="Thread"):
        nrPatents = len(pd_Patents)
        pd_Patents.reset_index(inplace=True, drop=True)
        self.driver = webdriver.Chrome(chrome_options=chromeOptions)
        
        if exitFlag:
            threadName.exit()
        
        Citing_Content = pd.DataFrame()
        patent_HTML = pd.DataFrame()
                             
        for i, row in pd_Patents.iterrows():
            logging.debug("Currently on row: {} / {}".format( i+1, nrPatents))
                        
            start = time.time()
            try:
                self.driver.get(row["result link"])
                   
                try:
                    WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h3[id=legalEvents] + div.responsive-table.style-scope.patent-result div.tbody')))
                except TimeoutException:
                    logging.debug("no legal Event")
                    pass
                currentHTML = self.driver.find_element_by_tag_name('body').get_attribute('innerHTML')  
               
     
                
                df2 = pd.DataFrame({"id":row["id"],"HTML":[currentHTML],"publication date":row["publication date"]})
                patent_HTML = patent_HTML.append(df2)
                
                logging.debug("threadID:", threadID,  "time in sec:", round(time.time() - start, 2), 'id:', row["id"], 'link', row["result link"])
                
            except WebDriverException as e:
                logging.exception(e)
                #reload webdriver
                self.driver = webdriver.Chrome(chrome_options=chromeOptions)
                
            
            except Exception as e: 
                logging.exception(e)
                end = time.time()
                logging.debug(i, "time in sec:", round(end - start, 2), 'exception on loading')
                pass
        self.driver.quit()
        return patent_HTML

            

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s',
                    )  
    file = "Sample"
    overall_start = time.time()
    'load patents from Company_patents:'
    
    cwd= os.getcwd()
    patentfile = cwd + '\\patents.h5'
    nrThreads = 4
     
    with HDFStore(patentfile, complevel=4) as store:     
        patent_info_store = store['Patent_info']
    
    #shortening PD for debugging
    #patent_info_store = patent_info_store.iloc[0:100]

     
    # ['id', 'title', 'assignee', 'inventor/author', 'priority date', 'filing/creation date', 'publication date', 'grant date', 'result link']
    count_Patents = len(patent_info_store)
   
    

    logging.info("Number of Patents to load: {} ".format(count_Patents))
    
    #Split the patents into a maximum number to save memory
    
    for i,patent_info in patent_info_store.groupby(np.arange(len(patent_info_store))//1000):    
        
        logging.info("{} / {} patents loaded".format(i*len(patent_info),len(patent_info_store)))
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
                
                #Appends the new records to the old records and drops duplicates 
                
                
                try:
                        with HDFStore(htmlfile, complevel=8) as store:
                            storePD = store['pdHTML']
                            storePD= storePD.append(df)
                            store.close()
                        #delete file to avoid missing support to free up space 
                        os.remove(htmlfile)
                except KeyError:
                        storePD = df       
                 
                storePD = storePD.drop_duplicates(subset=['id'], keep='last')   
                storePD = storePD.reset_index(drop=True)
                with HDFStore(htmlfile, complevel=8) as store2:
                    store2['pdHTML'] = storePD
                    store2.close()
    
    
    hours, rem = divmod(time.time() - overall_start, 3600)
    minutes, seconds = divmod(rem, 60)
    logging.info('time in h: {:0>2}:{:0>2}:{:05.2f} for {} records with {} threads'.format(int(hours),int(minutes),seconds,len(patent_info_store),nrThreads))
    logging.info('Please Wait.... Shutting Threads Down')
 
    