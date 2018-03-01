
import os
import sys
import threading
import time



from pandas.io.pytables import HDFStore
import numpy as np
import pandas as pd

import logging
from PatentScraper import CitesFromPatentReader




pd.options.display.encoding = sys.stdout.encoding



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
        self._return = CitesFromPatentReader.CitesFromPatentReader().openURL(self.scrapList,self.threadID, self.name)
        logging.debug("Exiting " + self.name)
    def join(self):
        threading.Thread.join(self)
        return self._return
            

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
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
    patent_info_store = patent_info_store.iloc[0:10]

     
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
        pdCites = pd.DataFrame()
        
        for i in range(0,nrThreads):
            allThreads.append(myThread(i, "Thread-" + str(i), urlLists[i])) 
        
        for thread in allThreads:
            thread.start()  
        
        for thread in allThreads:  
            pdCites = pdCites.append(thread.join())   
        
        
        
        #pdCites = pdCites.reset_index(drop=True)
        
        #pdCites["publication date"] = pd.to_datetime(pdCites["publication date"])
        
       #pdCites.index = pdCites["publication date"]
        
        
        
        logging.info(pdCites.to_string())
            
        if len(pdCites.index)>0: 
    
            htmlfile = cwd + '\\HDFStore\\Cites.h5'
            #
            
            #Appends the new records to the old records and drops duplicates 
            
            
            try:
                    with HDFStore(htmlfile, complevel=8) as store:
                        storePD = store['pdCites']
                        storePD= storePD.append(df)
                        store.close()
                    os.remove(htmlfile)
            except KeyError:
                    storePD = df       
             
            storePD = storePD.drop_duplicates(keep='last')   
            storePD = storePD.reset_index(drop=True)
            with HDFStore(htmlfile, complevel=8) as store2:
                store2['pdCites'] = storePD
                store2.close()
    
    end = time.time()
    
    runtime=round(end - overall_start, 2)/60/60
    
    hours, rem = divmod(time.time() - overall_start, 3600)
    minutes, seconds = divmod(rem, 60)
    logging.info('time in h: {:0>2}:{:0>2}:{:05.2f} for {} records cited by with {} threads'.format(int(hours),int(minutes),seconds,len(patent_info_store),nrThreads))
    logging.info('Please Wait.... Shutting Threads Down')
 
    