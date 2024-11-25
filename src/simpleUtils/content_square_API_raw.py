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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"data-lake-191d246e77ad.json"

bq_client = bigquery.Client()
gcs_client= storage.Client()


# headers= {"Authorization":"Bearer xxx","sq-Type":"application/json"} get from SM
prv_dy= (datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")
def data_model():
    endpoint_exportable_fields = "https://api.sqsquare.com/v1/"
    res= rq.get(endpoint_exportable_fields, headers=headers )
    df= pd.json_normalize(res.json()["payload"])
    # df.to_csv("testing2.csv")
def create_export_job():
    endpoint_create_job = 'https://api.sqsquare.com'
    data= {
    "name": "sessions_export_job",
    "format": "JSONL",
    "scope": "sessions",
    "frequency": {"value": "daily"},
    "fields": [
        {"fieldName":"project_id",},
        {"fieldName":"session_id",},
        {"fieldName":"session_date",},
        {"fieldName":"session_time",},
        {"fieldName":"device_id",},
        {"fieldName":"session_number_of_views",},
        {"fieldName":"session_number",},
        {"fieldName":"language",},
        {"fieldName":"os_version",},
        {"fieldName":"app_version",},
        {"fieldName":"model",},
        {"fieldName":"manufacturer",},
        {"fieldName":"screen_height",},
        {"fieldName":"screen_width",},
        {"fieldName":"density",},
        {"fieldName":"session_duration_msec",},
        {"fieldName":"session_number_of_transactions",},
        {"fieldName":"session_transactions_revenue_cents",},
        {"fieldName":"browser_name",},
        {"fieldName":"platform_version",},
        {"fieldName":"browser_major_version",},
        {"fieldName":"platform_name",},
        {"fieldName":"browser_version",},
        {"fieldName":"country_code",},
        {"fieldName":"city",},
        {"fieldName":"referer_url",},
        {"fieldName":"user_id",},
        {"fieldName":"user_id_unhashed",},
        {"fieldName":"app_events.event_type",},
        {"fieldName":"app_events.event_time",},
        {"fieldName":"app_events.event_name",},
        {"fieldName":"app_events.view_number",},
        {"fieldName":"is_excludable",},
        {"fieldName":"session_transaction_items.id",},
        {"fieldName":"session_transaction_items.product_id_hashed",},
        {"fieldName":"session_transaction_items.revenue_cents",},
        {"fieldName":"session_transaction_items.quantity",},
        {"fieldName":"session_transaction_items.sku",},
        {"fieldName":"session_transaction_items.catalog_id",},
        {"fieldName":"session_transaction_items.transaction_time",},
        {"fieldName":"session_transaction_items.transaction_view_number",},
        {"fieldName":"session_add_to_cart_items.sku",},
        {"fieldName":"session_add_to_cart_items.catalog_id",},
        {"fieldName":"session_add_to_cart_items.product_id_hashed",},
        {"fieldName":"session_add_to_cart_items.is_in_transaction",},
        {"fieldName":"session_add_to_cart_items.add_to_cart_time",},
        {"fieldName":"session_add_to_cart_items.add_to_cart_view_number",}
    ],
    "deviceLabel": "all"
    }
    response = rq.post(endpoint_create_job,headers=headers,json=data)
    print(response.text)
    print(response.json)
    print(response.status_code)

def list_all_jobs():
    endpoint_list_jobs= 'https://api.sqsquare.com/v1/exports'
    response = rq.get(endpoint_list_jobs,headers=headers)
    print(response.text)

def get_ran_jobs():
    endpoint_success_jobs = "https://api.sqsquare.com/v1/exports/successful-runs"
    response = rq.get(endpoint_success_jobs,headers=headers)
    print(response.text)
def get_custom_vars():
    endpoint_custom_vars = "https://api.sqsquare.com/v1/custom-vars"
    response = rq.get(endpoint_custom_vars,headers=headers)
    print(response.text)
def get_dynamic_vars():
    endpoint_dynamic_vars="https://api.sqsquare.com/v1/dynamic-var-keys?from=2023-01-01T00:00:00&to=2023-12-07T00:00:00"
    response = rq.get(endpoint_dynamic_vars,headers=headers)
    print(response)
    print(response.status_code)
    df=pd.json_normalize(response.json()["payload"])
    df.to_csv("testing3.csv")
def get_job_run_files(id):
    endpoint_get_files = f"https://api.sqsquare.com/v1/exports/{id}/runs"
    response= rq.get(endpoint_get_files,headers=headers)
    df = pd.json_normalize(response.json()['payload'])
    job_run_id=df[pd.to_datetime(df['startDate']).dt.strftime("%Y-%m-%d") == prv_dy]["jobRunId"][0]
    print(f"The job run id is {job_run_id}")
    # job_run_id= filterd_df["jobRunId"][0]
    get_job_run_parts_files(id,job_run_id)

def get_job_run_parts_files(id,runid):
    endpoint_get_files = f"https://api.sqsquare.com/v1/exports/{id}/runs/{runid}" 
    response = rq.get(endpoint_get_files,headers=headers)
    df= pd.json_normalize(response.json()["payload"]["files"])
    counter=0
    main_df=pd.DataFrame()
    for endpoint in df["url"]:
        # print(endpoint)
        res=rq.get(endpoint)
        # print(res.sq.decode("utf-8"))
        with gzip.GzipFile(fileobj=BytesIO(res.sq), mode='rb') as file:
            jsonl_data = file.read().decode('utf-8')
            parsed_data = [json.loads(line) for line in jsonl_data.split("\n") if line]
            df2=pd.DataFrame(parsed_data)
            main_df= pd.concat([main_df,df2],ignore_index=True)
            df2.to_csv(f"testing{counter}.csv")
            counter+=1
            # print(jsonl_data)
        print("--------------")
        # with open("testing3.txt","w+") as file:
        #     file.write(res.json())
        # df1=pd.json_normalize(res.json())
        # print(res.text)
        # df1.to_csv(f"sting{counter}.csv")
    # df.to_csv("esting4.csv")
    main_df.to_csv(f"/main_df.csv")
    # main_df.to_gbq(main_df,if_exists="replace")
    current_date=datetime.now().strftime("%Y%m%d")
    destination_table=f"BigData_Staging_NonProd.sq_square_Sessions_{current_date}"
    project_id="data-lake"
    main_df.to_gbq(destination_table=destination_table,project_id=project_id,if_exists="replace",progress_bar=True)

if __name__ == "__main__":
    # get_custom_vars()
    # get_dynamic_vars()
    # create_export_job()
    # list_all_jobs()
    # get_ran_jobs()
    get_job_run_files(1119)

    
    # get_job_run_files(1120)
    # get_job_run_parts_files(1119,130348)


