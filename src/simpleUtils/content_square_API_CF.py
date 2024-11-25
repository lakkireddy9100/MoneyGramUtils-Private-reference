import pandas_gbq as pdg
import requests as rq
import json
import pandas as pd
import gzip
from io import BytesIO
from datetime import datetime,timedelta
from google.cloud import bigquery
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"77ad.json"
bq_client = bigquery.Client()
gcs_client= storage.Client()
# headers= {"Authorization":"Bearer xxxxxx","Content-Type":"application/json"} get from secret manager
prv_dy= (datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")

def get_job_run_files(id):
    endpoint_get_files = f"https://api.contentsquare.com/v1/exports/{id}/runs"
    response= rq.get(endpoint_get_files,headers=headers)
    print(response)
    df = pd.json_normalize(response.json()['payload'])
    job_run_id=df[pd.to_datetime(df['startDate']).dt.strftime("%Y-%m-%d") == prv_dy]["jobRunId"][0]
    get_job_run_parts_files(id,job_run_id)

def get_job_run_parts_files(id,runid):
    endpoint_get_files = f"https://api.contentsquare.com/v1/exports/{id}/runs/{runid}" 
    response = rq.get(endpoint_get_files,headers=headers)
    print(response)
    df= pd.json_normalize(response.json()["payload"]["files"])
    counter=0
    main_df=pd.DataFrame()
    for endpoint in df["url"]:
        res=rq.get(endpoint)
        with gzip.GzipFile(fileobj=BytesIO(res.content), mode='rb') as file:
            jsonl_data = file.read().decode('utf-8')
            parsed_data = [json.loads(line) for line in jsonl_data.split("\n") if line]
            df2=pd.DataFrame(parsed_data)
            main_df= pd.concat([main_df,df2],ignore_index=True)
            counter+=1
    current_date=datetime.now().strftime("%Y%m%d")
    destination_table=f"BigData_Staging_NonProd.content_square_Sessions_{current_date}"
    project_id="data-lake"
    try:
        main_df.to_gbq(destination_table=destination_table,project_id=project_id,if_exists="replace",progress_bar=True)
        return "Uplaoded"
    except:
        return "Failed"
# def callable_main(request):

