import pandas as pd
import pyodbc
import json
import requests
from datetime import datetime, timedelta, date
import pymysql
from sqlalchemy import create_engine


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
        WHERE dimDepart.departName LIKE '%TW%' AND (customItem199__c LIKE '%C%' OR customItem199__c LIKE '%D%') and dimDepart.departName not LIKE '%TW-Z%'
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
 
#account_df = account_df[account_df['目標客戶類型'].notna()]   
    
'''
select related_contact 
'''

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
        "xoql": '''select name, customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, customItem8__c 公司代號,contactPhone__c__c 手機號碼,customItem45__c 修改日期 ,id 客戶關係連絡人, customItem98__c as 聯絡人代號 ,customItem74__c LINEID,customItem95__c 職務類別, customItem109__c 聯絡人勿擾選項, customItem42__c 連絡人資料無效,
        customItem24__c 關係狀態,customItem91__c 連絡人普查標籤, customItem132__c 連絡人普查貼標日期
        from customEntity22__c where customItem37__c  like '%TW%'
        ''',
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
    
contact_related1 = contact_related[contact_related['連絡人普查貼標日期']!='']
contact_related2 = contact_related[contact_related['連絡人普查貼標日期']=='']
contact_related1['連絡人普查貼標日期'] = contact_related1['連絡人普查貼標日期'].astype(float)
contact_related1['連絡人普查貼標日期'] = contact_related1['連絡人普查貼標日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
contact_related1['連絡人普查貼標日期'] = contact_related1['連絡人普查貼標日期'].dt.tz_convert('Asia/Taipei')
contact_related1['連絡人普查貼標日期'] = contact_related1['連絡人普查貼標日期'].dt.strftime('%Y-%m-%d')
contact_related = pd.concat([contact_related1,contact_related2])

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
        "xoql": '''select customItem6__c.name name, customItem2__c 最近上線K大上線日期, customItem30__c
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
    
K_visit_df['最近上線K大上線日期'] = pd.to_numeric(K_visit_df['最近上線K大上線日期'], errors='coerce')  
K_visit_df['最近上線K大上線日期'] = K_visit_df['最近上線K大上線日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
K_visit_df['最近上線K大上線日期'] = K_visit_df['最近上線K大上線日期'].dt.tz_convert('Asia/Taipei')
K_visit_df['最近上線K大上線日期'] = K_visit_df['最近上線K大上線日期'].dt.strftime('%Y-%m-%d')

K_visit_df['最近上線K大上線日期'] = pd.to_datetime(K_visit_df['最近上線K大上線日期'], errors='coerce')
K_visit_df = K_visit_df[pd.notna(K_visit_df['最近上線K大上線日期'])]
K_visit_df = K_visit_df.sort_values(by='最近上線K大上線日期', ascending=True) 

K_visit_df['customItem30__c']= K_visit_df['customItem30__c'].astype(str)
K_visit_df = K_visit_df[K_visit_df['customItem30__c'].str.contains("是")]
K_visit_df = K_visit_df.drop_duplicates(subset=['name'], keep='last')

contact_related = pd.merge(contact_related, K_visit_df, on = 'name', how = 'left')

K_invite = pd.merge(contact_related, account_df, on = '公司代號', how = 'inner')

K_invite['職務類別']= K_invite['職務類別'].astype(str)
K_invite = K_invite.loc[K_invite['職務類別'].str.contains("001|002|003|004|005|006|007|010|011|015")]
K_invite['關係狀態']= K_invite['關係狀態'].astype(str)
K_invite = K_invite[~K_invite['關係狀態'].str.contains("配合")]
K_invite['連絡人資料無效']= K_invite['連絡人資料無效'].astype(str)
K_invite = K_invite[~K_invite['連絡人資料無效'].str.contains("是")]
K_invite['聯絡人勿擾選項']= K_invite['聯絡人勿擾選項'].astype(str)
K_invite = K_invite[~K_invite['聯絡人勿擾選項'].str.contains("電訪")]
K_invite = K_invite[~K_invite['連絡人'].str.contains("測試")]

#K_invite.to_excel("C:/Users/11020856/Desktop/jupyter/test.xlsx",index = False) 
#K_invite1 = K_invite[K_invite['連絡人'].str.contains("洪修平",na=False)]
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
#df_merge.loc[((df_merge['MarkID'] == '000011') |(df_merge['MarkID'] == '000020')), '公司電話'] =''
K_invite.loc[K_invite['公司簡稱'].str.contains("搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|管制|非營業中|測試"), '公司電話'] = ''
K_invite['Inactive'] = K_invite['Inactive'].astype(str)
K_invite['Inactive'] = K_invite['Inactive'].str.strip()
K_invite.loc[K_invite['Inactive']=='True', '公司電話'] = ''

#公司電話、手機、聯絡人電話與IineID都缺的才刪除
K_invite = K_invite[~((K_invite['公司電話'] == '') & (K_invite['手機號碼'] == ''))]

#手機電話去重、加回
K_invite1 = K_invite[K_invite['手機號碼'].apply(lambda x: isinstance(x, str) and len(x) == 10 and x != '0000000000' and x.startswith('09'))]
K_invite1 = K_invite1.drop_duplicates(subset=['手機號碼'], keep='last')

# value_counts = K_invite1['公司代號'].value_counts()
# unique_values = value_counts[value_counts == 1].index.tolist()
# K_invite3 = K_invite1[K_invite1['公司代號'].isin(unique_values)]
# contact_related['關係狀態']= contact_related['關係狀態'].astype(str)
# contact_related1 = contact_related[contact_related['關係狀態'].str.contains('離職')]
# K_invite3 = K_invite3[(K_invite3['連絡人代號'].isin(contact_related1['連絡人代號'].drop_duplicates()))]
# K_invite1 = K_invite1[~(K_invite1['連絡人代號'].isin(K_invite3['連絡人代號'].drop_duplicates()))]

#K_invite2 = K_invite.drop_duplicates(subset=['連絡人代號'], keep='last')
K_invite2 = K_invite[~(K_invite['手機號碼'].isin(K_invite1['手機號碼'].drop_duplicates()))]
value_counts = K_invite2['公司代號'].value_counts()
unique_values = value_counts[value_counts == 1].index.tolist()
K_invite2 = K_invite2[K_invite2['公司代號'].isin(unique_values)]
contact_related['關係狀態']= contact_related['關係狀態'].astype(str)
contact_related1 = contact_related[contact_related['關係狀態'].str.contains('離職')]
K_invite2 = K_invite2[~(K_invite2['連絡人代號'].isin(contact_related1['連絡人代號'].drop_duplicates()))]

K_invite3 = pd.concat([K_invite1,K_invite2])

K_invite3['目標客戶類型']= K_invite3['目標客戶類型'].astype(str)
K_invite_經營 = K_invite3.loc[K_invite3['目標客戶類型'].str.contains("經營客戶",na = False)]
K_invite_開發 = K_invite3.loc[~K_invite3['目標客戶類型'].str.contains("經營客戶",na = False)]

K_invite_經營開發 = pd.concat([K_invite_經營,K_invite_開發])
K_invite_經營開發.to_excel("Z:/28_數據中心部/如苾/經營開發_all0115.xlsx",index = False) 
TW = K_invite_經營開發['name'].count()

server = '192.168.1.119'
database = 'bidb'
username = 'kdmis'
password = 'Kd0123456'
crm_con = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password) 
cursor = crm_con.cursor()

# cursor.execute("SELECT 日期, 台灣 FROM Daily_country_callnumber ")
# results = cursor.fetchall()
# results = [tuple(row) for row in results]
# column_names = ["日期", "台灣"]
# K = pd.DataFrame(results, columns=column_names)

today = date.today()
today = today.strftime("%Y-%m-%d")
TW = int(TW)

update_query = """
    UPDATE Daily_country_callnumber
    SET 台灣 = ?
    WHERE 日期 = ?
"""
cursor.execute(update_query, (TW, today))

cursor.commit()