import pandas as pd
import requests
import pyodbc
from datetime import datetime, timedelta
import pymysql
from sqlalchemy import create_engine

'''
ask token
'''    
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
from 銷售易 account
'''

url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
account_df = pd.DataFrame()
 
while True:
    data = {
        "xoql": '''
        select  id, SAP_CompanyID__c SAP公司代號
        from account
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    account_df = pd.concat([account_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']

'''
from 銷貨
'''
connection = pymysql.connect(
        host='192.168.1.253',  # 数据库地址
        port=3307,
        user='DATeam',          # 用户名
        password='Dateam@1234', # 密码
        database='db01',        # 数据库名称
        charset='utf8'       # 字符编码
    )
cursor = connection.cursor()
current_datetime = datetime.now()
target_date = current_datetime - timedelta(days=7)
seven_days_str = target_date.strftime('%Y/%m/%d')

cursor.execute(f"""SELECT 買方,未稅本位幣, 預計發貨日期 
               FROM sap_sales_data  where 預計發貨日期 >= '{seven_days_str}'
               """)

results = cursor.fetchall()
sap = pd.DataFrame(results)
sap.columns = ["SAP公司代號", "未稅本位幣", "customItem357__c" ]
sap = sap[sap['未稅本位幣']>0]
first_delivery_date = sap.groupby('SAP公司代號')['customItem357__c'].max().reset_index()
sap1 = pd.merge(sap, first_delivery_date, on=['SAP公司代號', 'customItem357__c'])
sap1 = sap1.drop_duplicates(subset=['SAP公司代號'], keep='last')
account_df1 = pd.merge(account_df, sap1, on=['SAP公司代號'])
account_df1['customItem357__c'] = pd.to_datetime(account_df1['customItem357__c'], errors='coerce')
account_df1['customItem357__c'] = account_df1['customItem357__c'].apply(lambda x: int(x.timestamp() * 1000) if pd.notnull(x) else None)
account_df1 = account_df1[["id", "customItem357__c" ]]

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
        "object": "account",
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
num_batches = (len(account_df1) + batch_size - 1) // batch_size
# Initialize a list to store responses
all_responses = []
# Initialize an index to keep track of the current batch
current_batch_index = 0

# While there are more batches to process
while current_batch_index < num_batches:
    start_index = current_batch_index * batch_size
    end_index = (current_batch_index + 1) * batch_size
    current_batch_df = account_df1.iloc[start_index:end_index]
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


#merged_df.to_excel("C:/Users/11020856/Desktop/jupyter/Sap客戶最後交易日0913.xlsx",index = False) 