import logging
import sys
import time

from selenium import webdriver
from selenium.common.exceptions import  TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import pandas as pd
from bs4 import BeautifulSoup as Soup

import urllib3
import certifi

chromeOptions = webdriver.ChromeOptions()
#prefs = {"profile.managed_default_content_settings.images":2}
#chromeOptions.add_experimental_option("prefs",prefs)
chromeOptions.add_argument("--headless")  


pd.options.display.encoding = sys.stdout.encoding


exitFlag = 0


        
class CitesFromPatentReader():
    def __init__(self):
        pass
        
    def openURL(self,pd_Patents,threadID='1', threadName="Thread"):
        nrPatents = len(pd_Patents)
        #pd_Patents.reset_index(inplace=True, drop=True)
        #self.driver = webdriver.Chrome(chrome_options=chromeOptions)
        
        if exitFlag:
            threadName.exit()
        
        
        patent_Cites = pd.DataFrame()
        patent_Cites.index.name = 'PatentId'
        count=0                     
        for patentId, row in pd_Patents.iterrows():
            count = count+1
            logging.debug("Currently on row: {} / {}".format( count, nrPatents))
            #i = i+threadID
            
           
            try:
                http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
                
                response = http.request('GET',row["result link"])
                output = response.data.decode('utf-8', 'ignore')
                soup = Soup(output, "html.parser")
                currentContent = soup.select('tr[itemprop=backwardReferences]  span[itemprop=publicationNumber]')
#                 with open('my_filehtml.html', 'w', encoding='utf-8') as fo:
#                     fo.write(soup.prettify())
                for cites in currentContent:
                    df = pd.DataFrame({"IsCiting":str(cites.getText())}, index=[patentId]) 
                    
                    patent_Cites = patent_Cites.append(df)
                
#                self.driver.get(row["result link"])
                
                
                
                   
#                 try:
#                     WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.ID, 'patentCitations')))
#                 except TimeoutException:
#                     logging.debug("no patentCitations")
#                     pass
#                 #currentHTML = self.driver.find_element_by_tag_name('body').get_attribute('innerHTML') 
#                 currentContent = self.driver.find_elements_by_css_selector("h3[id=patentCitations] +div.responsive-table.style-scope.patent-result div.tbody div span state-modifier a")                    
#                 
#                 for cites in currentContent:
#                     df = pd.DataFrame({"IsCiting":cites.text}, index=[patentId]) 
#                     logging.debug(df.to_string())
#                     patent_Cites = patent_Cites.append(df)
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
                logging.warn(e)
                #self.driver = webdriver.Chrome(chrome_options=chromeOptions)
            
            except Exception as e: 
                logging.warn(e,exc_info=True)
                print(sys.exc_info()[0])
                #self.driver = webdriver.Chrome(chrome_options=chromeOptions)
                
        #self.driver.quit()
        
        return patent_Cites

    