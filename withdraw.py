#!/usr/bin/env python

import pandas as pd
import json
import requests
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import numpy as np



date_to_convert = datetime(2025,3, 19)
timestamp = int(date_to_convert.timestamp() * 1000)

url = "https://login-p10.xiaoshouyi.com/auc/oauth2/token"
payload = {
    "grant_type": "password",
    "client_id": "a037eace03d67c0f77a26e74edcde9a1",
    "client_secret": "c50f98b7f5c5d362d1bf6eb9d3447692",
    "redirect_uri": "https://crm-p10.xiaoshouyi.com",
    "username": "tw0002@twkd.com.cn",
    "password": "Ywb314775877MoylQRYl"
}

response = requests.post(url, data=payload)
content = response.json()
ac_token = content["access_token"]


#####
##2904963933786093 寄後電訪
##3028348436713387 每日K大
####
'''
select from Tasks withdraw 
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
Tasks_df1 = pd.DataFrame()

while True:
    data = {
        "xoql": f'''
        select id,name,customItem10__c, customItem3__c,workflowStageName,approvalStatus,customItem8__c,customItem206__c
        ,customItem121__c,customItem65__c, customItem42__c.name 客戶關係連絡人編號,entityType
        from customEntity14__c
        where entityType in ('2904963933786093','3028348436713387') and createdBy = '3628254003531750' 
        and createdAt >= {timestamp} 
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    Tasks_df1 = pd.concat([Tasks_df1, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
Tasks_df1['customItem8__c']= Tasks_df1['customItem8__c'].astype(str)
Tasks_df1 = Tasks_df1.loc[Tasks_df1['customItem8__c'].str.contains("等待")]  
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem8__c'].str.contains("進行中")]  
Tasks_df1['customItem121__c']= Tasks_df1['customItem121__c'].astype(str)
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem121__c'].str.contains("已邀約")] 
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem3__c'].str.contains("K大視訊|已邀約")] 
Tasks_df1['customItem65__c']= Tasks_df1['customItem65__c'].astype(str)
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem65__c'].str.contains("已邀")] 
#Tasks_df1 = Tasks_df1.loc[Tasks_df1['customItem3__c'].str.contains("已邀")] 

#Tasks_df1 = Tasks_df1[Tasks_df1.duplicated(subset='客戶關係連絡人編號', keep=False)]
#Tasks_df1 = Tasks_df1.drop_duplicates(subset=['客戶關係連絡人編號'], keep='last')
#Tasks_df1 = pd.read_excel("C:/Users/11020856/Desktop/jupyter/70交辦管理-台灣_20240909103457649_廖珮均.xlsx", dtype='object')

'''
get procInstId
'''
Tasks_df2 = Tasks_df1[['id']]
base_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/history/filter?entityApiKey=customEntity14__c&dataId="

def fetch_data(data_id):
    print(data_id)
    api_url = f"{base_url}{data_id}&stageFlg=false"
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        api_data = response.json()
        if api_data["data"]:
          print(api_data["data"][-1]["procInstId"])
          return json.dumps({ "dataId": data_id, "procInstId": api_data["data"][-1]["procInstId"] })
          # df = pd.DataFrame(api_data["data"][0]["procInstId"], dtype=str)
        else:
            print("Data is empty")
            return
    else:
        print(f"Error accessing API for dataId {data_id}. Status code: {response.status_code}")
        # return pd.DataFrame()
        return
    

# Set the maximum number of threads you want to use
max_threads = 2  # You can adjust this based on your needs


with ThreadPoolExecutor(max_threads) as executor:
    # Use executor.map to asynchronously fetch data for each row in parallel
    dfs = list(executor.map(fetch_data, Tasks_df2['id']))
    # Add a time delay between each thread

json_data = [entry for entry in dfs if entry is not None]
Tasks_df2 = pd.json_normalize([json.loads(entry) for entry in json_data])


'''
withdraw_task
'''

def withdraw_task(row):
    data_id = row['dataId']
    task_id = row['procInstId']
    url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
    headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type": "application/json"
    }
    data = {
        "data": {
            "action": "withdraw",
            "entityApiKey": "customEntity14__c",
            "dataId": data_id,
            "procInstId": task_id
        }
    }

    response = requests.post(url_2, headers=headers, json=data)
    result = response.json()
    print(f"Response for dataId {data_id}: {result}")

# Assuming data_ids is your DataFrame
data_ids_df = Tasks_df2[['dataId', 'procInstId']]

# Create a thread for each row in the DataFrame
threads = []
for index, row in data_ids_df.iterrows():
    thread = threading.Thread(target=withdraw_task, args=(row,))
    threads.append(thread)
    thread.start()
    time.sleep(0.08)

# Wait for all threads to finish
for thread in threads:
    thread.join()

'''
ask bulk id
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/bulk/v2/job"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/json"
}
data = {
     "data": {
        "operation": "delete",####################################################################3
        "object": "customEntity14__c",
        "execOption": ["CHECK_RULE", "CHECK_DUPLICATE"]
    }
    
}
response = requests.post(url_2, headers = headers, json = data)
test = response.json()
bulk_id = test["result"]["id"]
print(bulk_id)

Tasks_df3 = Tasks_df2[['dataId']]
Tasks_df3 = Tasks_df3.rename(columns={'dataId':'id'})
#Tasks_df3 = Tasks_df[['id']]
'''
delete to crm if dataframe over 5000
'''
batch_size = 5000

# Calculate the number of batches
num_batches = (len(Tasks_df3) + batch_size - 1) // batch_size
# Initialize a list to store responses
all_responses = []
# Initialize an index to keep track of the current batch
current_batch_index = 0

# While there are more batches to process
while current_batch_index < num_batches:
    start_index = current_batch_index * batch_size
    end_index = (current_batch_index + 1) * batch_size
    current_batch_df = Tasks_df3.iloc[start_index:end_index]
    json_data = current_batch_df.to_dict(orient='records')
    
    url_2 = "https://api-p10.xiaoshouyi.com/rest/bulk/v2/batch"
    headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type":"application/json"
    }
    data = {
        "data": {
            "jobId": f"{bulk_id}",
            "datas": json_data
        }
    }
    response = requests.post(url_2, headers=headers, json=data)
    all_responses.append(response)
    current_batch_index += 1
for response in all_responses:
    pass