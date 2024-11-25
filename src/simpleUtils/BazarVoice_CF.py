from xml.etree import ElementTree as ET
import csv
from lxml import etree
import pandas as pd
import os
def return_data_model(child_element,parent_ele,data_dict)->dict:
    for sub_ele in child_element:
        if list(sub_ele):
            if str(sub_ele.tag).split("}")[1] =="UPCs":
                upcs_list=[]
                for ele in sub_ele.iter():
                    if ele.tag.strip() != "" and ele.text is not None:
                        upcs_list.append(ele.text)
                data_dict["UPCs"]=upcs_list
            else:
                data_dict=return_data_model(child_element=sub_ele,parent_ele=child_element,data_dict=data_dict)
        elif sub_ele.text:
            key_for_dict=etree.QName(sub_ele.tag).localname
            value_for_dict= sub_ele.text
            counter =1
            key_for_dict=etree.QName(child_element.tag).localname+"_"+key_for_dict
            while key_for_dict in data_dict.keys():
                if key_for_dict +f"_{counter}" not in data_dict.keys():
                    key_for_dict +=f"_{counter}"
                counter+=1
            data_dict[key_for_dict]=value_for_dict
    return data_dict

root=ET.parse("xml_data.xml").getroot()
count=0
unique_ele=set()
for element in root:
    unique_ele.add(element.tag)

category_list_of_dict=[]
product_list_of_dict=[]

for element in root:
    data_dict=return_data_model(element,root,{})
    if element.tag.split("}")[1]=="Category":
        category_list_of_dict.append(data_dict)
    else:
        product_list_of_dict.append(data_dict)
category_headers=set(keys for dictionary in category_list_of_dict for keys in dictionary)
# print(category_headers)
product_headers= set(keys for dictionary in product_list_of_dict for keys in dictionary)

for dictionary in product_list_of_dict:
    for keys in dictionary:
        try:
            dictionary[keys]= str(dictionary[keys]).encode('utf-8')
        except:
            continue

csv_path= "xml_file_category.csv"
with open(csv_path,"w",newline='',encoding="utf-8") as csv_file:
    writer=csv.DictWriter(csv_file,fieldnames=category_headers)
    writer.writeheader()
    for row in category_list_of_dict:
        writer.writerow(row)

csv_path_product= "xml_file_product.csv"
with open(csv_path_product,"w",newline='') as csv_file:
    writer=csv.DictWriter(csv_file,fieldnames=product_headers)
    writer.writeheader()
    for row in product_list_of_dict:
        # encoded_row={key:value.encode('latin-1').decode('utf-8') if isinstance(value,str) else value for key,value in row.items() }
        # writer.writerow(encoded_row)
        writer.writerow(row)
df=pd.read_csv("xml_file_product.csv",encoding='utf-8')
df_decoded=pd.DataFrame()
for col in df:
    df_decoded[col]= df[col].apply(lambda x: x[2:-1] if isinstance(x,str) else str(x))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=""

destination_table="BigData_Staging_NonProd.bazarvoice"
project_id = "data-lake"
df_decoded.to_gbq(destination_table=destination_table,project_id=project_id,if_exists="replace",progress_bar=True)

