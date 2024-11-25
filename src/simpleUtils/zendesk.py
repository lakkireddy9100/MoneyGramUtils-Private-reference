# import datetime
# import requests
# import pandas as pd
# from urllib.parse import urlencode
# import time
# import json
# import os
# url= 'https:.zendesk.com/api/v2/tickets.json?page[size]=100'
# user =
# pwd=
# response = requests.get(url, auth=(user, pwd))
# data=json.loads(response.text)
# with open("zendesk_data.txt","w") as file:
#     file.write(str(data))
# date_time= datetime.datetime(2023,5,8,21,5)


import requests
import pandas as pd
from urllib.parse import urlencode
import time
import json
import os
url= 'https://zendesk.com/api/v2/tickets.json?page[size]=100&page[after]=EifQ=='
user = '.com'+'/token'
pwd=
response = requests.get(url, auth=(user, pwd))
data=json.loads(response.text)
url= data['links']['next']
status = data['meta']['has_more']
counter=0
merged_df=pd.DataFrame()
while counter <= 1000:
        response= requests.get(url,auth=(user,pwd))
        data=json.loads(response.text)
        df=pd.DataFrame.from_dict(data['tickets'])
        if counter==0:
            merged_df=df
        else:
            merged_df= merged_df.concat(df,ignore_index=True)
        url= data['links']['prev']
        status = data['meta']['has_more']
        counter+=1
        print("Exception",counter)
        print(data['links']['prev'])