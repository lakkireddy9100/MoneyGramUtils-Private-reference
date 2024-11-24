import urllib.parse
import base64
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os
from google.cloud import bigquery
import pandas_gbq
# get from secret m,anager
# client_id =
# client_secret =
# access_key_id =
# access_key_secret =

def authenticate(client_id,client_secret,access_id,access_secret):

    client_id_encoded = urllib.parse.quote(client_id)
    client_secret_encoded = urllib.parse.quote(client_secret) 
    access_key_id_encoded =access_key_id 
    access_key_secret_encoded = access_key_secret
    string = client_id_encoded + ':' + client_secret_encoded 
    
    base64_encoded_string = base64.b64encode(string.encode()).decode() 
    url = 'https://cxone.niceincontact.com/a/t/x'
    payload = {"grant_type": "password", "username": access_key_id_encoded, "password": access_key_secret_encoded}
    headers = {'Authorization': 'Basic ' + base64_encoded_string, 'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(url, headers=headers, data=payload).json()
    return response['access_token']

def contacts_completed(api_token,startDate,endDate):
    headers={"Content-Type":"application/json",
             "Authorization":f"Bearer {api_token}"}
    next=f"https://api-c6.incontact.com/incontactapi/services/v29.0/contacts/completed?startDate={startDate}&endDate={endDate}"
    main_df= pd.DataFrame()
    while next is not None:
        endpoint_URL=next
        response = requests.get(endpoint_URL,headers=headers)
        with open("contacts_completed.txt","w+") as file:
          file.write(response.text)
        next=response.json()["_links"]["next"]
        df=pd.json_normalize(response.json()["completedContacts"])
        df["businessUnitId"]= response.json()["businessUnitId"]
        main_df= pd.concat([main_df,df],ignore_index=True)
    current_time= datetime.now()
    destination_table="BigData_Staging_NonProd.incontact_contacts_completed_{}".format(current_time.strftime("%Y%m%d"))
    project_id="data-lake"
    main_df.to_gbq(destination_table=destination_table,project_id=project_id,if_exists="replace")

def get_data(request):
    api_token = authenticate(client_id,client_secret,access_key_id,access_key_secret)
    # print(api_token)
    prev_day=datetime.now().date()- timedelta(days=1)
    startDate= datetime.combine(prev_day,datetime.min.time()).isoformat()
    endDate = datetime.combine(prev_day,datetime.max.time()).isoformat()
    print(startDate,endDate)
    contacts_completed(api_token=api_token,startDate=startDate,endDate=endDate)
    return "Uploaded"