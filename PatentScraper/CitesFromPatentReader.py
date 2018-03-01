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
from selenium.common.exceptions import NoSuchElementException, TimeoutException,WebDriverException
from pandas.io.pytables import HDFStore
from multiprocessing.pool import ThreadPool
import logging
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


chromeOptions = webdriver.ChromeOptions()
#prefs = {"profile.managed_default_content_settings.images":2}
#chromeOptions.add_experimental_option("prefs",prefs)
chromeOptions.add_argument("--headless")  


pd.options.display.encoding = sys.stdout.encoding


exitFlag = 0


        
class CitesFromPatentReader():
    def __init__(self):
        self.driver = webdriver.Chrome(chrome_options=chromeOptions)
        
    def openURL(self,pd_Patents,threadID='1', threadName="Thread"):
        nrPatents = len(pd_Patents)
        pd_Patents.reset_index(inplace=True, drop=True)
        self.driver = webdriver.Chrome(chrome_options=chromeOptions)
        
        if exitFlag:
            threadName.exit()
        
        
        patent_Cites = pd.DataFrame()
        patent_Cites.index.name = 'PatentId'
                             
        for i, row in pd_Patents.iterrows():
            logging.info("Currently on row: {} / {}".format( i+1, nrPatents))
            #i = i+threadID
            
            start = time.time()
            try:
                self.driver.get(row["result link"])
                   
                try:
                    WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h3[id=patentCitations] +div.responsive-table.style-scope.patent-result div.tbody')))
                except TimeoutException:
                    logging.warn("no patentCitations")
                    pass
                #currentHTML = self.driver.find_element_by_tag_name('body').get_attribute('innerHTML') 
                currentContent = self.driver.find_elements_by_css_selector("h3[id=patentCitations] +div.responsive-table.style-scope.patent-result div.tbody div span state-modifier a")                    
                
                for cites in currentContent:
                    df = pd.DataFrame({"IsCiting":cites.text}, index=[row["id"]]) 
                    logging.debug(df.to_string())
                    patent_Cites = patent_Cites.append(df)
                #currentHTML = self.cHTML
#                 tz=pd.read_html(currentHTML)
#                 
#                 cwd = os.getcwd()
#                 path = cwd + '\\tz.xlsx'
#                 writer = pd.ExcelWriter(path)
#                 tz.to_excel(writer, filename )
#                 writer.save()
                
                #df2 = pd.DataFrame({"id":row["id"],"HTML":[currentHTML],"publication date":row["publication date"]})
                
                
                
               
                
            except WebDriverException as e:
                print(e)
                self.driver = webdriver.Chrome(chrome_options=chromeOptions)
                
            
            except Exception as e: 
                print(e)
                end = time.time()
                print(i, "time in sec:", round(end - start, 2), 'exception on loading')
                pass
        self.driver.quit()
        return patent_Cites

    