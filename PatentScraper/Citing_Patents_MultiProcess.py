import threading
import time
from selenium import webdriver

import os
import pandas as pd
import numpy as np
import sys


from selenium.common.exceptions import NoSuchElementException
from pandas.io.pytables import HDFStore

import logging


    
def openURL(urlList,threadID='1', threadName="Thread"):
    print("Scrapping Nr of URLS", urlList.shape)       
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument("--headless")  
    driver = webdriver.Chrome(chrome_options=chromeOptions)
    Citing_Content = pd.DataFrame(data=None, columns=["Company", "Id", "Citing"])            
    for i in range (0, urlList.size):
        #i = i+threadID
        print(i)
        start = time.time()
        try:
            driver.get(urlList.iloc[i])
            currentContent = driver.find_element_by_css_selector("h3[id=citedBy] + div.responsive-table.style-scope.patent-result div.tbody").text                    
            currentContent = currentContent.split('\n')
            df = pd.DataFrame({"Company":Company.iloc[i],"Id":Id.iloc[i],"Citing":currentContent})
            Citing_Content = Citing_Content.append(df)
            end = time.time()
            print("ThreadID:", threadID, "row:", i, "time in sec:", round(end - start, 2), 'Cited by patents:', len(currentContent), 'urlList:', urlList.iloc[i])
            
        except NoSuchElementException:
            print(i, "time in sec:", round(time.time() - start, 2), 'Patent has not been cited.')
            
        
        except Exception as e: 
            print(e)
            end = time.time()
            print(i, "time in sec:", round(end - start, 2), 'exception on loading')
            
    #                     for index, row in currentContent.iterrows():            
    #                         row['Citing'] = row['Citing'].encode(sys.stdout.encoding, errors='replace')
    #                         try:
    #                             row['Citing'] = str(row['Citing'],'utf-8')
    #                         except: 
    #                             row['Citing'] = str(row['Citing'])
                
    cwd = os.getcwd()
             
    writer = pd.ExcelWriter(cwd +'\\Citing_Content_'+str(threadName)+'_'+str(threadID)+'.xlsx', engine='xlsxwriter')
    Citing_Content.reset_index(inplace=True)
    Citing_Content.to_excel(writer, sheet_name='Sheet1')
    workbook  = writer.book
    workbook.close()        
            
    

if __name__ == '__main__':
    file = "Sample"
    logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s',
                    )  
    
    
    
    'load patents from Company_patents:'
    patentfile = os.getcwd() + '\\patents.h5'
    nrThreads = 1
     
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
    start = time.time()
    #Split all URLs in equal chunks based on the number of expected threads
    urlLists = list()
    for g, df in URL.groupby(np.arange(len(URL))// (len(URL)/nrThreads)):
        print(df.shape)
        urlLists.append(df)
    
    # Create new threads
    from multiprocessing.dummy import Pool as ThreadPool 
    pool = ThreadPool(10) 
    pool.map(openURL, urlLists)
    pool.close()
    pool.join()
    
#     allThreads = list()
#     for i in range(nrThreads):
#         allThreads.append(myThread(i, "Thread-" + str(i), i, urlLists[i]))
#     
    # Start new Threads
    
#     
#     for thread in allThreads:
#         thread.start()        
#     
#     for i in allThreads:  
#         i.join()
#         
#    
#     
#     #TODO:Bugfixing:programm does not terminate!
    
    
    print('time in h:', round(time.time() - start, 2)/60/60)
    
    
    print(threading.active_count() )  
    
    
#     while threading.active_count() > 0:
#         time.sleep(0.1)
#         logging.debug(threading.active_count() )  
    
        