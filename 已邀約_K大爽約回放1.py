
import pandas as pd
import json
import requests
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import numpy as np

#1
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


'''
select from user
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace withyour actual access token
} 
data = {
    "xoql": "select id, name, dimDepart from user",
    #"xoql": "insert into contact(contactName, phone, email) values('貴夫人', '0912345678', 'kkdd@gmail.com')"
    "batchCount": 2000
}
response2 = requests.post(url_2, headers=headers, data = data)
ownerId = response2.json()
print(ownerId)
user_df = pd.json_normalize(ownerId["data"]["records"])  
user_df[user_df['name']=='李詠緁']
#4
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
Tasks_df = pd.DataFrame()

date_to_convert = datetime(2024,12, 1)
# # Calculate the Unix timestamp in milliseconds
timestamp = int(date_to_convert.timestamp() * 1000)

while True:
    data = {
        "xoql": f'''
        select id,name,customItem10__c, customItem3__c,customItem8__c,customItem49__c 區域,
        customItem121__c, customItem120__c 邀約日期, customItem45__c, customItem42__c, entityType,dimDepart,customItem11__c,customItem116__c
        from customEntity14__c
        where entityType = '3028348436713387' and customItem120__c >= {timestamp}
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
Tasks_df['customItem121__c']= Tasks_df['customItem121__c'].astype(str)
Tasks_df['customItem116__c']= Tasks_df['customItem116__c'].astype(str)
Tasks_df['邀約日期'] = Tasks_df['邀約日期'].astype(float)
Tasks_df['邀約日期'] = Tasks_df['邀約日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
Tasks_df['邀約日期'] = Tasks_df['邀約日期'].dt.tz_convert('Asia/Taipei')
Tasks_df['邀約日期'] = Tasks_df['邀約日期'].dt.strftime('%Y-%m-%d')
Tasks_df['邀約日期'] = pd.to_datetime(Tasks_df['邀約日期'], errors='coerce').dt.date

today = datetime.now().date()
Tasks_df1 = Tasks_df.loc[Tasks_df['customItem3__c'].str.contains("已邀|K大視訊")] 
Tasks_df1 = Tasks_df1.drop_duplicates(subset=['customItem42__c'], keep='last')
Tasks_df1 = Tasks_df1[Tasks_df1['邀約日期'] < today]


Tasks_df2 = Tasks_df1.loc[Tasks_df1['customItem3__c'].str.contains("已邀|K大視訊")] 
Tasks_df3 = Tasks_df1.loc[~Tasks_df1['customItem3__c'].str.contains("已邀|K大視訊|新客")]



'''
select from Sales list 
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
Sales_list = pd.DataFrame()

while True:
    data = {
        "xoql": '''
        select customItem1__c 電訪人員, customItem3__c 預估通數, customItem2__c 電訪人員類型, customItem9__c
        from customEntity42__c
        where customItem5__c = 1
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    Sales_list = pd.concat([Sales_list, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']

Sales_list['電訪人員類型']= Sales_list['電訪人員類型'].astype(str)
Sales_list['customItem9__c']= Sales_list['customItem9__c'].astype(str)
Sales_list = Sales_list[Sales_list['預估通數']!='']
Sales_list['預估通數']= Sales_list['預估通數'].astype(int)
Sales_list = Sales_list[Sales_list['預估通數']>0]
Sales_list = pd.merge(Sales_list, user_df, left_on = '電訪人員', right_on = 'id', how = 'left')
#Sales_list = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("產品")]
#Sales_list1 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("未接|產品",na=False)]
Sales_list2 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("未接",na=False)]
# Sales_list3 = Sales_list[Sales_list['電訪人員']=='3365366323730925']
# Sales_list1 = pd.concat([Sales_list1, Sales_list3]) 

##一般K大
# Tasks_df2['customItem120__c'] = today
# Tasks_df2['customItem121__c'] = '7'
# Tasks_df3 = Tasks_df2.loc[Tasks_df2['customItem116__c'].str.contains("經營",na=False)]
# Tasks_df3['電訪人員'] = np.random.choice(Sales_list1['電訪人員'], size=len(Tasks_df3))
# Tasks_df3 = pd.merge(Tasks_df3, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
# Tasks_df3 = Tasks_df3.drop(['customItem10__c'],axis=1)
# Tasks_df3 = Tasks_df3.rename(columns={'dimDepart_x':'dimDepart','電訪人員':'customItem10__c'})
# Tasks_df3['customItem206__c'] = '1'

##66
#Tasks_df4 = Tasks_df2.loc[~Tasks_df2['customItem116__c'].str.contains("經營",na=False)]

Tasks_df2['電訪人員'] = np.random.choice(Sales_list2['電訪人員'], size=len(Tasks_df2))
Tasks_df4 = pd.merge(Tasks_df2, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
Tasks_df4 = Tasks_df4.drop(['customItem10__c'],axis=1)
Tasks_df4 = Tasks_df4.rename(columns={'dimDepart_x':'dimDepart','電訪人員':'customItem10__c'})
Tasks_df4['customItem120__c'] = today
Tasks_df4['customItem121__c'] = '1'
#Tasks_df4['customItem206__c'] = '2'

Tasks_df3['電訪人員'] = np.random.choice(Sales_list2['電訪人員'], size=len(Tasks_df3))
Tasks_df5 = pd.merge(Tasks_df3, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
Tasks_df5 = Tasks_df5.drop(['customItem10__c'],axis=1)
Tasks_df5 = Tasks_df5.rename(columns={'dimDepart_x':'dimDepart','電訪人員':'customItem10__c'})
Tasks_df5['customItem120__c'] = today
Tasks_df5['customItem121__c'] = '1'

Tasks_df5 = pd.concat([Tasks_df4, Tasks_df5])
Tasks_df5 = Tasks_df5.reset_index(drop=True)
Tasks_df5 = Tasks_df5.rename(columns={'id_x':'id'})
# Tasks_df5.loc[Tasks_df5['區域'].str.contains("Z"), 'customItem10__c'] = '3365366323730925'
# Tasks_df5.loc[Tasks_df5['區域'].str.contains("Z"), 'customItem206__c'] = '1'



index_length = len(Tasks_df5)
values = [['1']] * index_length
Tasks_df5['customItem115__c'] = values
# grouped = Tasks_df5.groupby('customItem42__c')['customItem115__c'].agg(list).reset_index()
# K_invite_task = pd.merge(Tasks_df5, grouped, on = 'customItem42__c', how = 'left')
# K_invite_task = K_invite_task.drop(['customItem115__c_x'],axis=1)
# K_invite_task = K_invite_task.rename(columns={'customItem115__c_y':'customItem115__c'})
K_invite_task = Tasks_df5[['customItem42__c','entityType','customItem120__c','customItem3__c','customItem121__c','customItem10__c','dimDepart','customItem11__c','customItem45__c','customItem115__c']]
K_invite_task = K_invite_task.astype(str)
K_invite_task = K_invite_task[K_invite_task['customItem10__c']!='']
K_invite_task['customItem10__c'].value_counts()



#2
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
        "operation": "insert",####################################################################3
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
num_batches = (len(K_invite_task) + batch_size - 1) // batch_size
# Initialize a list to store responses
all_responses = []
# Initialize an index to keep track of the current batch
current_batch_index = 0

# While there are more batches to process
while current_batch_index < num_batches:
    start_index = current_batch_index * batch_size
    end_index = (current_batch_index + 1) * batch_size
    current_batch_df = K_invite_task.iloc[start_index:end_index]
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



# '''
# select from Tasks withdraw 
# '''
# url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
# headers = {
#     "Authorization": f"Bearer {ac_token}",
#     "Content-Type":"application/x-www-form-urlencoded"
#     # Replace with your actual access token
# }
# queryLocator = ''
# Tasks_df = pd.DataFrame()

# date_to_convert = datetime(2024,6, 19)
# # # Calculate the Unix timestamp in milliseconds
# timestamp = int(date_to_convert.timestamp() * 1000)

# while True:
#     data = {
#         "xoql": f'''
#         select id,name,customItem10__c, customItem3__c,customItem8__c,customItem49__c 區域,
#         customItem121__c, customItem120__c 邀約日期, customItem45__c, customItem42__c, entityType,dimDepart,customItem11__c
#         from customEntity14__c
#         where entityType = '3028348436713387' and customItem120__c >= {timestamp}
#         ''',
#         "batchCount": 2000,
#         "queryLocator": queryLocator
#     }
#     response = requests.post(url_2, headers=headers, data=data)
#     crm = response.json()
#     data = pd.DataFrame(crm["data"]["records"])
#     Tasks_df = pd.concat([Tasks_df, data], ignore_index=True, sort=False)
    
#     if not crm['queryLocator']:
#         break
#     queryLocator = crm['queryLocator']
    
# Tasks_df['customItem8__c']= Tasks_df['customItem8__c'].astype(str)
# Tasks_df = Tasks_df.loc[Tasks_df['customItem8__c'].str.contains("等待")]  
# Tasks_df = Tasks_df[Tasks_df.duplicated(subset='customItem42__c', keep=False)]
# Tasks_df = Tasks_df.drop_duplicates(subset=['customItem42__c'], keep='last')

#3,5
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


################################








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

#Tasks_df3 = Tasks_df2[['dataId']]
#Tasks_df3 = Tasks_df3.rename(columns={'dataId':'id'})
Tasks_df3 = Tasks_df1[['id']]
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