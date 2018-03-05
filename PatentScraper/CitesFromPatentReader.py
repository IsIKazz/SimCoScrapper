import logging
import sys
import pandas as pd
from bs4 import BeautifulSoup as Soup

import urllib3
import certifi


pd.options.display.encoding = sys.stdout.encoding

exitFlag = 0
        
class CitesFromPatentReader():
    def __init__(self):
        pass
        
    def openURL(self,pd_Patents,threadID='1', threadName="Thread"):
        nrPatents = len(pd_Patents)
        
        if exitFlag:
            threadName.exit()
                
        patent_Cites = pd.DataFrame()
        patent_Cites.index.name = 'PatentId'
        count=0                     
        for patentId, row in pd_Patents.iterrows():
            count = count+1
            logging.debug("Currently on row: {} / {}".format( count, nrPatents))
           
            try:
                http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
                
                response = http.request('GET',row["result link"])
                output = response.data.decode('utf-8', 'ignore')
                soup = Soup(output, "html.parser")
                currentContent = soup.select('tr[itemprop=backwardReferences]  span[itemprop=publicationNumber]')

                for cites in currentContent:
                    df = pd.DataFrame({"IsCiting":str(cites.getText())}, index=[patentId])                     
                    patent_Cites = patent_Cites.append(df)
            
            except Exception as e: 
                logging.warn(e,exc_info=True)                
        
        return patent_Cites

    