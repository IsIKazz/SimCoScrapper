'''
Created on Oct 19, 2017

@author: FB
'''

import datetime
from pandas.io.pytables import HDFStore
import Scraper
import os


if __name__ == '__main__':
    
    '''TODO: Dynamische Abfrage, bis zu welchem Datum die Datenbank bereits Patente hat'''
    startdate = datetime.date(2004, 3, 13)
    waiting_time = 50 # Waiting time to not overstrain google
    cwd = os.getcwd()
    
    download_dir = cwd + "\\PatentScraper"
    
    Patent_info = Scraper().downloadCSV(startdate = startdate
                                        , waiting_time = 50
                                        , download_dir = download_dir
                                        , DaysTillStore = 10)

