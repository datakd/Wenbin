import pandas as pd
import json
import requests
import time
import threading
from datetime import datetime
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
user_df[user_df['name']=='翁偉倫']

'''
select from 發放明細
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
gift_df = pd.DataFrame()

date_to_convert = datetime(2024,12, 1)
# # Calculate the Unix timestamp in milliseconds
timestamp = int(date_to_convert.timestamp() * 1000)

while True:
    data = {
        "xoql": f'''
        select id customItem152__c,name 型錄發放申請編號,customItem26__c, account__c.Phone__c 公司電話,customItem106__c
        from customEntity25__c
        where createdAt >= {timestamp} and entityType = '2905332731124037'
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    gift_df = pd.concat([gift_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
    
gift_df['customItem106__c']= gift_df['customItem106__c'].astype(str)   
gift_df = gift_df.loc[~gift_df['customItem106__c'].str.contains("退件")]
gift_df1 = gift_df[(gift_df['customItem26__c']=='')&(gift_df['公司電話']!='')]
gift_df1 = gift_df1.drop_duplicates(subset=['公司電話'], keep='last')
gift_df1 = gift_df1[(gift_df1['公司電話']!='0')&(gift_df1['公司電話']!='0-')&(gift_df1['公司電話']!='-')]
gift_df2 = gift_df.drop_duplicates(subset=['customItem26__c'], keep='last')
gift_df = pd.concat([gift_df1,gift_df2])
gift_df = gift_df.drop_duplicates(subset=['型錄發放申請編號'], keep='last')
gift_df10 = gift_df[gift_df['型錄發放申請編號']=='CSR00320908']
gift_df10 = gift_df[gift_df['customItem26__c']=='0926979641']


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
       select id,name,customItem10__c.name name, customItem3__c,customItem8__c,customItem152__c.name 型錄發放申請編號,
       customItem121__c, customItem45__c, customItem42__c, entityType,customItem49__c 區域代碼,customItem11__c,
       customItem57__c,customItem59__c,customItem39__c,customItem153__c, customItem116__c 目標客戶類型
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
#Tasks_df = Tasks_df.loc[Tasks_df['customItem8__c'].str.contains("等待")]  
Tasks_df['customItem121__c'] = '1'
Tasks_df = pd.merge(Tasks_df, gift_df, on = '型錄發放申請編號', how = 'inner')
Tasks_df10 = Tasks_df[Tasks_df['型錄發放申請編號']=='CSR00354981']
Tasks_df10 = Tasks_df[Tasks_df['customItem42__c']=='3023856934834064']


Tasks_df['customItem115__c'] = '1'
Tasks_df = Tasks_df.drop_duplicates(subset=['customItem42__c'], keep=False)
#Tasks_df = Tasks_df.drop_duplicates(subset=['公司電話'], keep=False)
#Tasks_df = Tasks_df.drop_duplicates(subset=['customItem42__c'], keep='last')
grouped = Tasks_df.groupby('customItem42__c')['customItem115__c'].agg(list).reset_index()
Tasks_df = pd.merge(Tasks_df, grouped, on = 'customItem42__c', how = 'left')
Tasks_df = Tasks_df.drop(['customItem115__c_x'],axis=1)
Tasks_df = Tasks_df.rename(columns={'customItem115__c_y':'customItem115__c'})
Tasks_df['customItem157__c'] = '1'

Tasks_df['區域代碼'] = Tasks_df['區域代碼'].str[-2:]
Tasks_df = Tasks_df.loc[~Tasks_df['customItem3__c'].str.contains("未接聽|沒收到|不清楚")]
#Tasks_df = Tasks_df.loc[~Tasks_df['customItem8__c'].str.contains("完成")]  

today = datetime.now().date()

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
        select customItem1__c.name 電訪人員, customItem3__c 預估通數, customItem2__c 電訪人員類型, dimDepart.departName 區域代碼, customItem9__c
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
Sales_list1 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("未接")]
#Sales_list1 = Sales_list1.loc[Sales_list1['customItem9__c'].str.contains("一般",na=False)]


# Tasks_df = Tasks_df.loc[~Tasks_df['區域代碼'].str.contains("Z")]
# Tasks_df1= Tasks_df[Tasks_df['customItem45__c']!='']
# Tasks_df2 = Tasks_df[Tasks_df['customItem45__c']=='']


# Tasks_df2= Tasks_df.loc[Tasks_df['區域代碼'].str.contains("Z")]
# Tasks_df3 = Tasks_df2[:100]
# Tasks_df3['電訪人員'] = '李詠緁'
# Tasks_df3['customItem206__c'] = '3'
# Tasks_df2 = Tasks_df2[~Tasks_df2['customItem42__c'].isin(Tasks_df3['customItem42__c'].drop_duplicates())]
# K_invite1 = pd.merge(Tasks_df3, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
# K_invite1['customItem120__c'] = today
# K_invite1.rename(columns={'id_y': 'customItem10__c'}, inplace=True)
# K_invite1= K_invite1[['customItem42__c','entityType','customItem120__c','customItem3__c','customItem121__c',
#                       'customItem10__c','dimDepart','customItem11__c','customItem115__c', 'customItem45__c',
#                       'customItem152__c','customItem57__c','customItem59__c','customItem39__c','customItem153__c','customItem157__c','customItem206__c']]


#未接區
# invited_people1 = []

# for index, row in Sales_list1.iterrows():
#     person = row['電訪人員']  # Assuming 'person' is the column name
#     count = row['預估通數']  # Assuming 'count' is the column name

#     # Extend the invited_people list with 'person' repeated 'count' times
#     if not pd.isna(count):
#         invited_people1.extend([person] * int(count))
# invited_people1 = pd.DataFrame({'客服主任': invited_people1})
# invited_people1 = invited_people1.sample(frac=1).reset_index(drop=True)

# invited_people1 = pd.concat([invited_people1])
# Tasks_df.reset_index(drop=True, inplace=True)
# K_invite = pd.concat([Tasks_df,invited_people1],axis=1)
# K_invite = K_invite.rename(columns={'客服主任':'電訪人員'})
# K_invite = K_invite[K_invite['電訪人員'].notnull()]
# K_invite = pd.merge(K_invite, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
# K_invite.rename(columns={'id_y': 'customItem10__c'}, inplace=True)
# K_invite['customItem120__c'] = today
# Tasks_df1 = Tasks_df[~Tasks_df['customItem42__c'].isin(K_invite['customItem42__c'].drop_duplicates())]

balanced_list = np.tile(Sales_list1['電訪人員'], -(-len(Tasks_df) // len(Sales_list1)))  # Repeat and cover all rows
np.random.shuffle(balanced_list)  # Shuffle for randomness
Tasks_df['電訪人員'] = balanced_list[:len(Tasks_df)]
K_invite = pd.merge(Tasks_df, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
K_invite = K_invite.rename(columns={'dimDepart_x':'dimDepart','id_y':'customItem10__c'})
K_invite['customItem120__c'] = today
Tasks_df1 = Tasks_df[~Tasks_df['customItem42__c'].isin(K_invite['customItem42__c'].drop_duplicates())]
K_invite['customItem10__c'].value_counts()

# balanced_list = np.tile(Sales_list1['電訪人員'], -(-len(Tasks_df2) // len(Sales_list1)))  # Repeat and cover all rows
# np.random.shuffle(balanced_list)  # Shuffle for randomness
# Tasks_df2['電訪人員'] = balanced_list[:len(Tasks_df2)]
# K_invite1 = pd.merge(Tasks_df2, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
# K_invite1 = K_invite1.rename(columns={'dimDepart_x':'dimDepart','id_y':'customItem10__c'})
# K_invite1['customItem120__c'] = today
# Tasks_df2 = Tasks_df2[~Tasks_df2['customItem42__c'].isin(K_invite1['customItem42__c'].drop_duplicates())]
# K_invite1['customItem10__c'].value_counts()


# K_invite['目標客戶類型']= K_invite['目標客戶類型'].astype(str)

# K_invite2 = K_invite.loc[K_invite['目標客戶類型'].str.contains("經營客戶",na=False)]
# K_invite3 = K_invite.loc[~K_invite['目標客戶類型'].str.contains("經營客戶",na=False)]

# K_invite2['customItem206__c'] = '1'
# K_invite3['customItem206__c'] = '2'

K_invite_task = pd.concat([K_invite])
#K_invite_task = pd.concat([K_invite1])
K_invite_task = K_invite_task[K_invite_task['customItem42__c'].notna()]
K_invite_task = K_invite_task[['customItem42__c','entityType','customItem120__c','customItem3__c','customItem121__c',
                      'customItem10__c','dimDepart','customItem11__c','customItem115__c', 'customItem45__c',
                      'customItem152__c','customItem57__c','customItem59__c','customItem39__c','customItem153__c','customItem157__c']]

K_invite_task = K_invite_task.astype(str)
K_invite_task['customItem10__c'].value_counts()


##產品顧問
# K_invite1 = []
# for _, row in Sales_list1.iterrows():
#     people = row['電訪人員']
#     count = row['預估通數']
    
#     Tasks_df.reset_index(drop=True, inplace=True)
    
#     # Convert count to an integer
#     count = int(count)
    
#     # Get matching rows using slicing
#     matching_rows = Tasks_df[Tasks_df['電訪人員'] == people].head(count)
    
#     K_invite1.append(matching_rows)

# K_invite1 = pd.concat(K_invite1)
# K_invite1.reset_index(drop=True, inplace=True)
# K_invite1 = pd.merge(K_invite1, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
# K_invite1.rename(columns={'id_y': 'customItem10__c'}, inplace=True)
# K_invite1['customItem120__c'] = today
# Tasks_df1 = Tasks_df[~Tasks_df['customItem42__c'].isin(K_invite1['customItem42__c'].drop_duplicates())]
# K_invite1 = K_invite1[['customItem42__c','entityType','customItem120__c','customItem3__c','customItem121__c',
#                      'customItem10__c','dimDepart','customItem11__c','customItem115__c', 'customItem45__c',
#                      'customItem152__c','customItem57__c','customItem59__c','customItem39__c','customItem153__c','customItem157__c']]

# K_invite_task = pd.concat([K_invite,K_invite1])
# K_invite_task = K_invite_task[K_invite_task['customItem42__c'].notna()]
# K_invite_task = K_invite_task.astype(str)


#Tasks_df1.to_excel("Z:/28_數據中心部/如苾/last_寄後_invite.xlsx",index=False)




##########補名單
#Tasks_df1 = pd.read_excel("Z:/28_數據中心部/如苾/last_寄後_invite.xlsx", dtype='object')
Tasks_df1= Tasks_df.loc[~Tasks_df['區域代碼'].str.contains("Z")]
Tasks_df1 = Tasks_df

Tasks_df2 = Tasks_df1[:14]
Tasks_df1 = Tasks_df1[~Tasks_df1['customItem42__c'].isin(Tasks_df2['customItem42__c'].drop_duplicates())]
Tasks_df2['電訪人員'] = '張淇雁'
K_invite_task = pd.merge(Tasks_df2, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
K_invite_task.rename(columns={'id_y': 'customItem10__c'}, inplace=True)
K_invite_task['customItem120__c'] = today
K_invite_task = K_invite_task[['customItem42__c','entityType','customItem120__c','customItem3__c','customItem121__c',
                      'customItem10__c','dimDepart','customItem11__c','customItem115__c', 'customItem45__c',
                      'customItem152__c','customItem57__c','customItem59__c','customItem153__c','customItem157__c','customItem39__c']]
K_invite_task = K_invite_task[K_invite_task['customItem42__c'].notnull()]
K_invite_task = K_invite_task.astype(str)

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




'''
select from Tasks 
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
Tasks_df = pd.DataFrame()

while True:
    data = {
        "xoql": f'''
        select id,name, customItem10__c, customItem3__c, approvalStatus
        from customEntity14__c 
        where entityType = '2904963933786093' and createdAt >= {timestamp} 
        and customItem10__c != '2660038858067951' and customItem10__c != '3221972041390421'
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
    
Tasks_df['approvalStatus']= Tasks_df['approvalStatus'].astype(str)
Tasks_df = Tasks_df.loc[Tasks_df['approvalStatus'].str.contains("撤回|提交")] 
#Tasks_df = Tasks_df.loc[Tasks_df['approvalStatus'].str.contains("提交")] 
#Tasks_df = Tasks_df[Tasks_df['customItem3__c']=='寄後電訪交辦']
         
'''
submit_task
'''

status_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task/actions/preProcessor"
task_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type": "application/json"
    }

def preProcessor(process_ID):
    status_body = {
        "data": {
            "action": "submit",
            "entityApiKey": "customEntity14__c",
            "dataId": process_ID }}

    response = requests.post(status_url, headers=headers, json=status_body)
    crm_json = response.json()['data']
    return crm_json

tasks_df_last_row = Tasks_df.iloc[-1]
approval_status = preProcessor(tasks_df_last_row['id'])

def submit_task(row):
    data_id = row['id']
    task_id = row['customItem10__c']
    url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
    headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type": "application/json"
    }

    # Updated nextAssignees/ccs list

    data = {
        "data": {
            "action": "submit",
            "entityApiKey": "customEntity14__c",
            "dataId": data_id,
            "procdefId": approval_status['procdefId'],
            "nextTaskDefKey": approval_status['nextTaskDefKey'],
            "nextAssignees": [task_id],
            "ccs": [task_id]  
        }
    }

    response = requests.post(url_2, headers=headers, json=data)
    result = response.json()
    print(f"Response for dataId {data_id}: {result}")

# Assuming data_ids is your DataFrame
data_ids_df = Tasks_df[['id', 'customItem10__c']]

# Create a thread for each row in the DataFrame
threads = []
for index, row in data_ids_df.iterrows():
    thread = threading.Thread(target=submit_task, args=(row,))
    threads.append(thread)
    thread.start()
    time.sleep(0.08)

# Wait for all threads to finish
for thread in threads:
    thread.join() 
    
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
        select id,name,customItem10__c, customItem3__c,workflowStageName,approvalStatus,customItem8__c
        ,customItem121__c,createdAt
        from customEntity14__c
        where entityType = '2904963933786093' and customItem10__c != '3221972041390421'
        and customItem10__c != '2660038858067951' and createdAt > {timestamp}
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
# Tasks_df1['approvalStatus']= Tasks_df1['approvalStatus'].astype(str)
# Tasks_df1 = Tasks_df1.loc[Tasks_df1['approvalStatus'].str.contains("审批中")]  
Tasks_df1['customItem3__c']= Tasks_df1['customItem3__c'].astype(str)
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem3__c'].str.contains("F類客戶|未接聽|沒收到|不清楚")]

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


