
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
        try:
            self._return = CitesFromPatentReader.CitesFromPatentReader().openURL(self.scrapList,self.threadID, self.name)
        except Exception as e: 
            logging.warn(e)
            self._return = None
        logging.debug("Exiting " + self.name)
    def join(self):
        threading.Thread.join(self)
        return self._return
            

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%d-%m-%Y:%H:%M:%S'
                    )  
    file = "Sample"
    overall_start = time.time()
    'load patents from Company_patents:'
    
    cwd= os.getcwd()
    patentfile = cwd + '\\Patent_info_12_8_2013-16_8_2013.h5'
    resultFile = cwd + '\\HDFStore\\Cites.h5'
    nrThreads = 4
     
    with HDFStore(patentfile, complevel=8) as store:
        patent_info_store = store['Patent_info']
        patent_info_store = patent_info_store.set_index('Patent_id')
        
        '''shortening PD for debugging'''
        #patent_info_store = patent_info_store.iloc[0:10000]
    # ['id', 'title', 'assignee', 'inventor/author', 'priority date', 'filing/creation date', 'publication date', 'grant date', 'result link']
        count_Patents = len(patent_info_store)
        store.close()
    
    try:
        with HDFStore(resultFile, complevel=8) as store:
            initialPd = store['pdCites']
            logging.info("Nr of Records stored: {}".format(len(initialPd)))
            visitedPatent = initialPd.index.drop_duplicates().get_values()   
            logging.info("Number of Patents total: {}".format(patent_info_store.shape[0]))
            try:
                patent_info_store = patent_info_store.drop(visitedPatent)
            except ValueError:
                logging.warn("No values to drop")
                         
            store.close()
        
    except KeyError:
        initialPd = pd.DataFrame() 
        
    logging.info("Number of Patents to be scraped: {}".format(patent_info_store.shape[0]))
    nrPatents = 0
    #Split the patents into a maximum number to save memory
    
    for __,patent_info in patent_info_store.groupby(np.arange(len(patent_info_store))//400):    
        #Split all URLs in equal chunks based on the number of expected threads    
        
        urlLists = list()
        for __, df in patent_info.groupby(np.arange(len(patent_info))// (len(patent_info) /nrThreads)):
            
            urlLists.append(df)
        
        # Create new threads
        allThreads = list()
        pdCites = pd.DataFrame()
        try:
            for i in range(nrThreads):
                allThreads.append(myThread(i, "Thread-" + str(i), urlLists[i])) 
            
            for thread in allThreads:
                thread.start()  
            
            for thread in allThreads:  
                pdCites = pdCites.append(thread.join()) 
        except Exception as e: 
                logging.exception(e,exc_info=True)
                
            
        newRecords = len(pdCites)
        nrPatents = nrPatents + len(patent_info)
            
        
            
            
            
        storePD= initialPd.append(pdCites)
        logging.debug("Nr of Records before {} plus {} = {}".format(len(initialPd),len(pdCites), len(storePD)))                        
        storePD = storePD.reset_index().drop_duplicates(keep='last').set_index('index') 
        
        logging.debug("Nr of Records after duplicates removed {}".format(len(storePD)))         
        hours, rem = divmod(time.time() - overall_start, 3600)
        minutes, seconds = divmod(rem, 60)
        logging.info('{:0>2}:{:0>2}:{:04.1f} for {} of {} patents checked. {} new of now total {} records added'
                     .format(int(hours),int(minutes),seconds,nrPatents,len(patent_info_store),newRecords, len(storePD)))
            
            
        if newRecords>0: 
            '''Appends the new records to the old records and drops duplicates'''     
             
            #storePD = storePD.reset_index(drop=True)
            with HDFStore(resultFile, mode="w", complevel=8) as store2:
                store2['pdCites'] = storePD
                store2.close()
            initialPd= storePD
    end = time.time()
    
    runtime=round(end - overall_start, 2)/60/60
    
    hours, rem = divmod(time.time() - overall_start, 3600)
    minutes, seconds = divmod(rem, 60)
    logging.info('time in h: {:0>2}:{:0>2}:{:05.2f} for {} new records cited by with {} threads'.format(int(hours),int(minutes),seconds,len(patent_info_store),nrThreads))
    logging.info('Please Wait.... Shutting Threads Down')
 
    