'''
Created on Oct 13, 2017

@author: FB
'''
'Dann alles csv-files zusammenfuehren (per company)'

import datetime
import pandas as pd
import numpy as np

Patent_info = pd.DataFrame(data=None, columns=["id", "title", "assignee", "inventor/author", "priority date", "filing/creation date", 
                                             "publication date", "grant date", "result link"])

today = datetime.date.today()

for i in range(1000, 2000):
    time_delta_before = datetime.timedelta(days=i)
    File = today-time_delta_before
    File = File.strftime("%Y%m%d")
    print(i, File)
    Data = pd.read_csv(str(File)+"_priority.csv", skiprows=(1),header=(0))
    id = pd.DataFrame(Data, columns=["id"])
    title = pd.DataFrame(Data, columns=["title"])
    assignee = pd.DataFrame(Data, columns=["assignee"])
    inventor_author = pd.DataFrame(Data, columns=["inventor/author"])
    priority_date = pd.DataFrame(Data, columns=["priority date"])
    filing_creation_date = pd.DataFrame(Data, columns=["filing/creation date"])
    publication_date = pd.DataFrame(Data, columns=["publication date"])
    grant_date = pd.DataFrame(Data, columns=["grant date"])
    result_link = pd.DataFrame(Data, columns=["result link"])
    Current_Patent_Content = np.concatenate((id, title, assignee, inventor_author, priority_date, filing_creation_date, publication_date, grant_date,
                                             result_link), axis=1)
    df = pd.DataFrame(Current_Patent_Content, columns=["id", "title", "assignee", "inventor/author", "priority date", "filing/creation date", 
                                             "publication date", "grant date", "result link"])
    
    Patent_info = Patent_info.append(df)

writer = pd.ExcelWriter('1000-2000_filing.xlsx', engine='xlsxwriter',options={'strings_to_urls': False})
Patent_info.to_excel(writer, sheet_name='Sheet1')
workbook  = writer.book
worksheet = writer.sheets['Sheet1']
workbook.close()

print("done")
