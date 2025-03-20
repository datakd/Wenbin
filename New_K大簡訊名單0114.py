import pandas as pd
import pyodbc
import json
import requests
from datetime import datetime, timedelta


url = "https://login-p10.xiaoshouyi.com/auc/oauth2/token"
payload = {
    "grant_type": "password",
    "client_id": "",
    "client_secret": "",
    "redirect_uri": "https://crm-p10.xiaoshouyi.com",
    "username": "",
    "password": ""
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
        select id accountId,accountCode__c 公司代號, dimDepart.departName 資料區域群組名稱 ,
        customItem197__c  公司簡稱, Phone__c 公司電話,
        customItem278__c Inactive, SAP_CompanyID__c SAP公司代號, customItem202__c 寄送地址,
        customItem199__c 公司型態名稱,customItem291__c 公司勿擾選項, customItem311__c 公司公用標籤, customItem322__c 目標客戶類型, parentAccCode1__c 關聯公司代號
        from account
        where dimDepart.departName like '%TW%'  and (customItem199__c like '%C%' or customItem199__c like '%D%' or customItem199__c = 'SE')
    
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
select related_contact 
'''
 ### 要加入Disable 職務類別customItem95__c?
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
contact_related = pd.DataFrame()
 
while True:
    data = {
        "xoql": "select customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, customItem8__c 公司代號,contactPhone__c__c 手機號碼,customItem12__c 信箱, updatedAt 修改日期 ,id 客戶關係連絡人, customItem98__c as 聯絡人代號 ,customItem74__c LINEID,customItem95__c 職務類別, customItem36__c 聯絡人勿擾選項, customItem42__c 連絡人資料無效, customItem24__c 關係狀態 from customEntity22__c where customItem37__c  like '%TW%'",
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    contact_related = pd.concat([contact_related, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']


K_invite = pd.merge(contact_related, account_df, on = '公司代號', how = 'inner')
K_invite['職務類別']= K_invite['職務類別'].astype(str)
K_invite = K_invite.loc[K_invite['職務類別'].str.contains("001|002|003|004|005|006|007|010|011|015")]
K_invite['關係狀態']= K_invite['關係狀態'].astype(str)
K_invite = K_invite[~K_invite['關係狀態'].str.contains("配合")]
K_invite['連絡人資料無效']= K_invite['連絡人資料無效'].astype(str)
K_invite = K_invite[~K_invite['連絡人資料無效'].str.contains("是")]
K_invite['聯絡人勿擾選項']= K_invite['聯絡人勿擾選項'].astype(str)
K_invite = K_invite[~K_invite['聯絡人勿擾選項'].str.contains("電訪|簡訊")]


#### 還有手機與電話是否要處理 

K_invite['修改日期'] = pd.to_datetime(K_invite['修改日期'], unit='ms')
K_invite['修改日期'] = K_invite['修改日期'].dt.strftime('%Y-%m-%d %H:%M:%S')  
K_invite = K_invite.sort_values(by='修改日期', ascending=False) 

###清除公司無效
connection_string = "Driver={Devart ODBC Driver for ASE};Database=PRD;Server=192.168.1.211;UID=sapsa;PWD=kd29003039;Port=4901;String Types=Unicode"

connection = pyodbc.connect(connection_string)

sql ='''SELECT SAPSR3.KNA1.KUNNR ,
                  SAPSR3.TKUKT.VTEXT
                      FROM SAPSR3.KNA1
                      LEFT JOIN SAPSR3.TKUKT
                      ON SAPSR3.KNA1.KUKLA=SAPSR3.TKUKT.KUKLA
                      AND SAPSR3.TKUKT.MANDT ='800'
                      AND SAPSR3.TKUKT.SPRAS = 'M'
    '''

df2 = pd.read_sql_query(sql, connection)
connection.close()
df2 = df2.dropna(subset=['VTEXT'])
df2.columns = ["SAP公司代號", "SAP信用管制"]
K_invite = pd.merge(K_invite, df2 ,on ='SAP公司代號',how='left')
K_invite.loc[K_invite['SAP信用管制'] == '管制', '公司電話'] =''


K_invite.loc[K_invite['公司簡稱'].str.contains("搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效拜訪|資料不全"), '公司電話'] = ''
K_invite['Inactive'] = K_invite['Inactive'].astype(str)
K_invite['Inactive'] = K_invite['Inactive'].str.strip()
K_invite.loc[K_invite['Inactive']=='True', '公司電話'] = ''

#公司電話、手機、聯絡人電話與IineID都缺的才刪除
K_invite = K_invite[~((K_invite['公司電話'] == '') & (K_invite['手機號碼'] == ''))]

#手機電話去重、加回
K_invite1 = K_invite[K_invite['手機號碼'].apply(lambda x: isinstance(x, str) and len(x) == 10 and x != '0000000000' and x.startswith('09'))]
K_invite1 = K_invite1.drop_duplicates(subset=['手機號碼'], keep='last')


'''
from K大預約表
'''

url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
K_visit_df = pd.DataFrame()
 
while True:
    data = {
        "xoql": '''select name,customItem30__c 是否舉行 ,customItem19__c 連絡人代號, customItem2__c 預約日期
        from customEntity23__c
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    K_visit_df = pd.concat([K_visit_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
    
K_visit_df['預約日期'] = pd.to_numeric(K_visit_df['預約日期'], errors='coerce')  
K_visit_df['預約日期'] = K_visit_df['預約日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
K_visit_df['預約日期'] = K_visit_df['預約日期'].dt.tz_convert('Asia/Taipei')
K_visit_df['預約日期'] = K_visit_df['預約日期'].dt.strftime('%Y-%m-%d')

current_datetime = datetime.now()
target_date = current_datetime - timedelta(days=30)

K_visit_df['預約日期'] = pd.to_datetime(K_visit_df['預約日期'], errors='coerce')
K_visit_df = K_visit_df[pd.notna(K_visit_df['預約日期'])]
K_visit_df = K_visit_df[K_visit_df['預約日期'] >= target_date]
K_visit_df['是否舉行']= K_visit_df['是否舉行'].astype(str)
K_visit_df = K_visit_df[K_visit_df['是否舉行'].str.contains("是")]

K_invite1 = K_invite1[~K_invite1['連絡人代號'].isin(K_visit_df['連絡人代號'].drop_duplicates())]

K_invite1.to_excel("C:/Users/11020856/Desktop/jupyter/K大簡訊名單_all.xlsx",index = False) 

