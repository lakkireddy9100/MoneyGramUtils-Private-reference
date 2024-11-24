import requests as rq
import pandas as pd
import json 
import csv
# csv_writer=csv.writer()

def normalize_json(json_data) -> dict:
    data_dict={}
    data_dict['id']=json_data['header'][0]['id'] 
    data_dict['tracking_number']=json_data['header'][0]['tracking_number']
    for key,item in json_data['header'][0]['actionBox'].items():
        data_dict[key]=item
    for key,item in json_data['header'][0]['courier'].items():
        if  not isinstance(item,dict):
            data_dict[key]=item
    for key,item in json_data['header'][0]['last_delivery_status'].items():
        data_dict[key]=item
    data_dict['delay']=json_data['header'][0]['delay']
    data_dict['exception']=json_data['header'][0]['exception']
    for key, item in json_data['header'][0]['delivery_info'].items():
        data_dict[key]=item
    return data_dict

order_detail_list=[]

with open("order_numbers.csv",'r') as csv_file:
    lines=csv_file.readlines()
    for line in lines:
        order_num=line.strip()
        response=rq.get(f"https://api.parcellab.com/x/x/x/checkpoints?lang=en&orderNo={order_num}&user=1617002")
        if response.status_code ==200:
            data_dict=normalize_json(response.json())
            data_dict['order_number']=order_num
            # print(data_dict)
            order_detail_list.append(data_dict)
        else:
            print(line)
            print(order_num,"error")

df=pd.DataFrame(order_detail_list)
df.to_csv("order_details_csv.csv",index=False, float_format='{:f}'.format, encoding='utf-8' )
