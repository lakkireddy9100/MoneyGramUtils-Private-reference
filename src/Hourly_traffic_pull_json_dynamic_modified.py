import requests as rq
import io
from google.cloud import storage
import base64
import pandas as pd
import os
from datetime import datetime
from datetime import timedelta
import json
from requests.auth import HTTPBasicAuth
from datetime import timezone
from datetime import timedelta
from google.cloud import bigquery
import pandas_gbq

current_time= datetime.now()
minute_data = (datetime.now(timezone.utc)).minute
hour_level_date = (datetime.now(timezone.utc)) - timedelta(minutes=minute_data) - timedelta(hours=2) + timedelta(minutes=15)
prior_hour_level_data= hour_level_date- timedelta(hours=3)
hour_level_date =hour_level_date.strftime("%Y%m%d%H%M")
prior_hour_level_data=prior_hour_level_data.strftime("%Y%m%d%H%M")

def hello_http():
    global current_time
    client=storage.Client()
    bucket="gs_bigdata_landing"
    project="data-lake"
    start_time = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y%m%d%H%M")
    end_time = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y%m%d%H%M")
    # api_url = f"https://stws.shoppertrak.com/EnterpriseFlash/v1.0/traffic/15min/site/all?start_time={start_time}&end_time={end_time}&detail=zone&total_property_only=false"
    url = "https://stws.shoppertrak.com/EnterpriseFlash/v1.0/traffic//x/x/x/xall?start_time={}&end_time={}&detail=zone&total_property_only=false".format(prior_hour_level_data,hour_level_date)
    username =
    password=
    
    client=bigquery.Client()
    credentials = f"{username}:{password}"
    credentials_base64 = base64.b64encode(credentials.encode()).decode()
    auth_header = f"Basic {credentials_base64}" 
    headers = {
      "Authentication" : auth_header
    }
    response = rq.get(url,auth=HTTPBasicAuth(username,password))
    if response.status_code ==200:
      with open("jsonfile.txt","w+") as file:
        file.write(response.text)
      print("Data fetched from API")
      data= response.json()
      data_list=[]
      for stores in data.keys():
          for store in data[stores]:
            storeID= store["storeID"]
            zones=store["zones"]
            for zone in zones:
              zoneID= zone["zoneName"]
              traffics = zone["traffic"]
              for traffic in traffics:
                startTime= traffic["startTime"]
                enters= traffic["enters"]
                exits = traffic["exits"]
                code = traffic["code"]
                data_list.append([storeID,zoneID,startTime,enters,exits,code])
      df=pd.DataFrame(data_list,columns=["storeID","zoneID","startTime","enters","exits","code"])
      destination_table="BigData_Staging_NonProd.Traffic{}".format(current_time.strftime("%Y%m%d"))
      project_id = "data-lake"
      print("Uplaoding to BQ")
      df.to_gbq(destination_table=destination_table,project_id=project_id,if_exists="replace",progress_bar=True)
      # json_str= json.dumps(data)
      # destination= 'test_file.json'
      # bucket= client.get_bucket(bucket)
      # blob = bucket.blob(destination)
      # blob.upload_from_file(io.BytesIO(json_str.encode()),content_type="application/json")
      return "Uploaded"
    else:
      return "Failed" + response.text 

if __name__=="__main__":
   print(hello_http())
