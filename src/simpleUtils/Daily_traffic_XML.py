import pandas as pd
import requests as rq
import xml.etree.ElementTree as ET
import os
from datetime import datetime
from datetime import timedelta
from requests.auth import HTTPBasicAuth
import pandas_gbq
from google.cloud import bigquery
from google.cloud import storage
def hello_http():
    current_time= datetime.now()
    prv_dy= (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    url = "https://stws.shoppertrscking.com/x/x/x/x".format(prv_dy)
    # username = move to credentials .ini
    # DB .pem files in Secret Manager already
    # password= move to credentials file .ini
    response = rq.get(url, auth=HTTPBasicAuth(username,password))
    print(response.text)
    if response.status_code == 200:
        bucket="gs_bigdata_landing"
        project="data-lake"
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="1d246e77ad.json"
        # Service account usage in .yaml workflow file
        client=storage.Client()
        root= ET.fromstring(response.text)
        print(response.text)
        field_vals= root.findall(".//site")
        csv_col= []
        for site in field_vals:
            siteID=site.get("siteID")
            date_ele=site.find(".//date")
            date_val= date_ele.get("dateValue")
            traffics= site.findall(".//traffic")
            for traffic in traffics:
                csv_col.append([siteID,date_val,traffic.get("code"),traffic.get("enters"),traffic.get("exits"),traffic.get("startTime")])
        df= pd.DataFrame(csv_col,columns=["siteID","date_val","code","enters","exits","startTime"])
        destination_table="BigData_Staging_NonProd.Traffic_daily{}".format(current_time.strftime("%Y%m%d"))
        project_id = "data-lake"
        print("Uploading to BQ")
        df.to_gbq(destination_table=destination_table,project_id=project_id,if_exists="replace",progress_bar=True)
        return "uploaded"
    else:
        return "Not connected"

if __name__ == "__main__":
    print(hello_http())

    