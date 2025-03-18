import pandas as pd
import json
import requests
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np

url = "https://login-p10.xiaoshouyi.com/auc/oauth2/token"
payload = {
    "grant_type": "password",
    "client_id": "a037eace03d67c0f77a26e74edcde9a1",
    "client_secret": "c50f98b7f5c5d362d1bf6eb9d3447692",
    "redirect_uri": "https://crm-p10.xiaoshouyi.com",
    "username": "11020856@twkd.com",
    "password": "11020856zAOdbVRj"
}

response = requests.post(url, data=payload)
content = response.json()
ac_token = content["access_token"]



'''
select from Tasks 廣發型錄
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
Tasks_df = pd.DataFrame()

date_to_convert = datetime(2024,12, 1)
# # Calculate the Unix timestamp in milliseconds
timestamp = int(date_to_convert.timestamp() * 1000)

while True:
    data = {
        "xoql": f'''
        select id,name,customItem10__c.name name,customItem3__c,customItem42__c,customItem8__c,customItem120__c, createdAt
        from customEntity14__c
        where entityType = '2904963933786093' and customItem120__c >= {timestamp}
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    Tasks_df = pd.concat([Tasks_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']

Tasks_df['customItem8__c']= Tasks_df['customItem8__c'].astype(str)
Tasks_df = Tasks_df.loc[Tasks_df['customItem8__c'].str.contains("等待")]  
Tasks_df['customItem120__c'] = Tasks_df['customItem120__c'].astype(float)
Tasks_df['customItem120__c'] = Tasks_df['customItem120__c'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
Tasks_df['customItem120__c'] = Tasks_df['customItem120__c'].dt.tz_convert('Asia/Taipei')
Tasks_df['customItem120__c'] = Tasks_df['customItem120__c'].dt.strftime('%Y-%m-%d')
Tasks_df['customItem120__c'] = pd.to_datetime(Tasks_df['customItem120__c'])
Tasks_df['createdAt'] = Tasks_df['createdAt'].astype(float)
Tasks_df['createdAt'] = Tasks_df['createdAt'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
Tasks_df['createdAt'] = Tasks_df['createdAt'].dt.tz_convert('Asia/Taipei')
Tasks_df['createdAt'] = Tasks_df['createdAt'].dt.strftime('%Y-%m-%d')
Tasks_df['createdAt'] = pd.to_datetime(Tasks_df['createdAt'])
Tasks_df1 = Tasks_df.loc[Tasks_df['customItem3__c'].str.contains("未接聽")]
Tasks_df2 = Tasks_df.loc[Tasks_df['customItem3__c'].str.contains("沒收到|不清楚")]


working_days_to_subtract = 3
current_date = datetime.now() 
# Function to check if a date is a weekend (Saturday or Sunday)
def is_weekend(date):
    return date.weekday() >= 5  # Saturday and Sunday are 5 and 6

# Calculate the date three working days ago
while working_days_to_subtract > 0:
    current_date -= timedelta(days=1)
    if not is_weekend(current_date):
        working_days_to_subtract -= 1
        

Tasks_df3 = Tasks_df2[Tasks_df2['createdAt'] < current_date]
Tasks_df4 = Tasks_df2[Tasks_df2['createdAt'] > current_date]

today = datetime.now()
three_day = datetime.now() + timedelta(days=2)

timestamp1 = int(today.timestamp() * 1000)
timestamp2 = int(three_day.timestamp() * 1000)

Tasks_df = pd.concat([Tasks_df1, Tasks_df3])
Tasks_df = Tasks_df[Tasks_df['customItem120__c'] < today]
Tasks_df['customItem120__c'] = timestamp1
Tasks_df4['customItem120__c'] = timestamp2

Tasks_df = pd.concat([Tasks_df, Tasks_df4])
Tasks_df = Tasks_df.drop_duplicates(subset=['customItem42__c'], keep=False)
Tasks_df = Tasks_df[Tasks_df['customItem120__c']!='']

Tasks_df = Tasks_df[['id','customItem120__c']]

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
        "operation": "update",####################################################################3
        "object": "customEntity14__c",
        "execOption": ["CHECK_RULE", "CHECK_DUPLICATE"]
    }
    
}
response = requests.post(url_2, headers = headers, json = data)
test = response.json()
bulk_id = test["result"]["id"]
print(bulk_id)

'''
insert to crm if dataframe over 5000
'''
batch_size = 5000

# Calculate the number of batches
num_batches = (len(Tasks_df) + batch_size - 1) // batch_size
# Initialize a list to store responses
all_responses = []
# Initialize an index to keep track of the current batch
current_batch_index = 0

# While there are more batches to process
while current_batch_index < num_batches:
    start_index = current_batch_index * batch_size
    end_index = (current_batch_index + 1) * batch_size
    current_batch_df = Tasks_df.iloc[start_index:end_index]
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

