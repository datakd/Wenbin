
from sqlalchemy import create_engine
import pandas as pd
import json
import requests
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pygsheets
import numpy as np

date_to_convert = datetime(2025,1, 1)
timestamp = int(date_to_convert.timestamp() * 1000)
target = ['經營客戶']


file_path = '/Users/wenbinyang/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/D43DB696294C275B6EDADD711B526B65/Caches/Files/2025-02/1b9db63ca750a41f2dab964a26873a76/廣發3300名單.xlsx'
Pass_user = pd.read_excel(file_path, sheet_name='廣發3300', usecols=['聯絡人代號'])['聯絡人代號'].dropna().tolist()


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
    "Content-Type": "application/x-www-form-urlencoded"
}

queryLocator = ''
user_df = pd.DataFrame()

while True:
    data = {
        "xoql": "select id, name, dimDepart from user",
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    
    response = requests.post(url_2, headers=headers, data=data)
    result = response.json()

    batch_df = pd.DataFrame(result["data"]["records"])
    user_df = pd.concat([user_df, batch_df], ignore_index=True, sort=False)

    if not result['queryLocator']:
        break
    queryLocator = result['queryLocator']

# 篩選特定名字
user_df[user_df['name'] == '楊文斌']
    
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
        "xoql": '''select name 客戶關係連絡人GS, 
        contactCode__c__c 連絡人代號, id 客戶關係連絡人, customItem24__c 關係狀態,dimDepart, customItem8__c customItem11__c
        from customEntity22__c where customItem37__c  like '%TW%' and customItem37__c  not like 'TW-Z%'
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




'''
select 展示館預約/參訪記錄(customEntity43__c)
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}


queryLocator = ''
museum = pd.DataFrame()
while True:
    data = {
        "xoql": f'''
        select customItem23__c.name 客戶關係連絡人,
        customItem51__c 是否到訪,customItem1__c 預約參訪日期
        from customEntity43__c
        where customItem1__c >= {timestamp}
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator       
    }
    
    response2 = requests.post(url_2, headers=headers, data = data)
    exist = response2.json()
    museum_data = pd.DataFrame(exist["data"]["records"])
    museum = pd.concat([museum, museum_data], ignore_index=True, sort=False)
    
    if not exist['queryLocator']:
        break
    queryLocator = exist['queryLocator']
    
# museum['預約參訪日期'] = museum['預約參訪日期'].astype(float)
# museum['預約參訪日期'] = museum['預約參訪日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
# museum['預約參訪日期'] = museum['預約參訪日期'].dt.tz_convert('Asia/Taipei')
# museum['預約參訪日期'] = museum['預約參訪日期'].dt.strftime('%Y-%m-%d')
# museum = museum.sort_values(by='預約參訪日期', ascending=True) 
museum = museum[museum['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)]
Pass_user = list(set(Pass_user + museum['客戶關係連絡人'].dropna().tolist()))





'''
select  K大預約參會人(customEntity24__c)
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}


queryLocator = ''
booking = pd.DataFrame()
while True:
    data = {
        "xoql": f'''
        select customItem2__c.name 客戶關係連絡人,
        customItem8__c 是否到訪,customItem31__c 預約日期
        from customEntity24__c
        where customItem1__c >= {timestamp}
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator       
    }
    
    response2 = requests.post(url_2, headers=headers, data = data)
    exist = response2.json()
    booking_data = pd.DataFrame(exist["data"]["records"])
    booking = pd.concat([booking, booking_data], ignore_index=True, sort=False)
    
    if not exist['queryLocator']:
        break
    queryLocator = exist['queryLocator']
    
# museum['預約參訪日期'] = museum['預約參訪日期'].astype(float)
# museum['預約參訪日期'] = museum['預約參訪日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
# museum['預約參訪日期'] = museum['預約參訪日期'].dt.tz_convert('Asia/Taipei')
# museum['預約參訪日期'] = museum['預約參訪日期'].dt.strftime('%Y-%m-%d')
# museum = museum.sort_values(by='預約參訪日期', ascending=True) 
booking = booking[booking['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)]
Pass_user = list(set(Pass_user + booking['客戶關係連絡人'].dropna().tolist()))






'''
select from trackingrecord 
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}


queryLocator = ''
tracking = pd.DataFrame()
while True:
    data = {
        "xoql": f'''
        select
        customItem48__c 客戶關係連絡人,
        customItem177__c 無效電訪類型,
        customItem40__c 最近聯繫時間,
        customItem128__c 觸客類型,
        customItem55__c 手機號碼

        from customEntity15__c
        where customItem40__c >= {timestamp}
        and customItem118__c like '%TW%'
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator       
    }
    
    response2 = requests.post(url_2, headers=headers, data = data)
    exist = response2.json()
    data = pd.DataFrame(exist["data"]["records"])
    tracking = pd.concat([tracking, data], ignore_index=True, sort=False)
    
    if not exist['queryLocator']:
        break
    queryLocator = exist['queryLocator']
tracking['最近聯繫時間'] = tracking['最近聯繫時間'].astype(float)
tracking['最近聯繫時間'] = tracking['最近聯繫時間'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
tracking['最近聯繫時間'] = tracking['最近聯繫時間'].dt.tz_convert('Asia/Taipei')
tracking['最近聯繫時間'] = tracking['最近聯繫時間'].dt.strftime('%Y-%m-%d')
tracking = tracking.sort_values(by='最近聯繫時間', ascending=True) 
current_datetime = datetime.now()
target_date = current_datetime - timedelta(days=30)

tracking['最近聯繫時間'] = pd.to_datetime(tracking['最近聯繫時間'])
tracking5 = tracking[tracking['最近聯繫時間'] >= target_date]

'''
select from trackingrecord 
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}


queryLocator = ''
tracking1 = pd.DataFrame()
while True:
    data = {
        "xoql": f'''
        select
        customItem48__c 客戶關係連絡人,
        customItem177__c 無效電訪類型,
        customItem40__c 最近聯繫時間,
        customItem128__c 觸客類型,
        customItem55__c 手機號碼,
        customItem57__c

        from customEntity15__c
        where customItem40__c >= {timestamp}
        and customItem118__c like '%TW%'
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator       
    }
    
    response2 = requests.post(url_2, headers=headers, data = data)
    exist = response2.json()
    data = pd.DataFrame(exist["data"]["records"])
    tracking1 = pd.concat([tracking1, data], ignore_index=True, sort=False)
    
    if not exist['queryLocator']:
        break
    queryLocator = exist['queryLocator']
tracking1['最近聯繫時間'] = tracking1['最近聯繫時間'].astype(float)
tracking1['最近聯繫時間'] = tracking1['最近聯繫時間'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
tracking1['最近聯繫時間'] = tracking1['最近聯繫時間'].dt.tz_convert('Asia/Taipei')
tracking1['最近聯繫時間'] = tracking1['最近聯繫時間'].dt.strftime('%Y-%m-%d')
tracking1 = tracking1.sort_values(by='最近聯繫時間', ascending=True) 
tracking1['最近聯繫時間'] = pd.to_datetime(tracking1['最近聯繫時間'])
tracking1['無效電訪類型']= tracking1['無效電訪類型'].astype(str)
tracking1 = tracking1[(tracking1['無效電訪類型'].str.contains("未接|接通"))]
# tracking1['觸客類型']= tracking1['觸客類型'].astype(str)
# tracking1 = tracking1[(tracking1['觸客類型'].str.contains("B"))]

#1118
# tracking['無效電訪類型']= tracking['無效電訪類型'].astype(str)
# tracking7 = tracking[(tracking['無效電訪類型'].str.contains("無效|客戶退休"))]
# tracking7.to_excel("Z:/28_數據中心部/如苾/K_invite_無效.xlsx",index = False) 

# K_invite_無效 =  pd.read_excel("Z:/28_數據中心部/如苾/K_invite_無效.xlsx", dtype='object')
# K_invite_無效 = pd.concat([tracking5,K_invite_無效])







經營開發 = pd.read_excel("/Users/wenbinyang/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/D43DB696294C275B6EDADD711B526B65/Caches/Files/2025-02/66b7ae3577fb7ecdaf9af1f7361534d9/K大名單監控具體數據_偉倫.xlsx", dtype='object')
經營開發 = 經營開發[~經營開發['name'].isin(Pass_user)]
經營開發 = 經營開發[經營開發['目標客戶類型'].str.contains('|'.join(target), na=False)]

經營開發1 = pd.merge(經營開發, tracking, on = '客戶關係連絡人', how = 'left')
經營開發1 = 經營開發1.sort_values(by='最近聯繫時間', na_position='first') 
經營開發1 = 經營開發1[~經營開發1['客戶關係連絡人'].isin(tracking5['客戶關係連絡人'].drop_duplicates())]
經營開發1 = 經營開發1.rename(columns={'手機號碼_x':'手機號碼'})
經營開發1 = 經營開發1[~經營開發1['手機號碼'].isin(tracking5['手機號碼'].drop_duplicates())]
經營開發1 = 經營開發1.drop_duplicates(subset=['客戶關係連絡人'], keep='last')

經營開發2 = pd.merge(經營開發, tracking1, on = '客戶關係連絡人', how = 'inner')
經營開發2 = 經營開發2.sort_values(by='最近聯繫時間', na_position='first') 
經營開發2 = 經營開發2.rename(columns={'手機號碼_x':'手機號碼'})
經營開發2 = 經營開發2.drop_duplicates(subset=['客戶關係連絡人'], keep='last')

經營開發 = pd.concat([經營開發1,經營開發2])
'''
from 銷售易 其他電拜訪 and createdBy = '2660038927554928'
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
        select name,customItem118__c 公司代號, customItem42__c 客戶關係連絡人, createdAt 創建日期, customItem8__c 執行狀態
        from customEntity14__c
        where  entityType != '3028348436713387'   and createdAt >= {timestamp}
        
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

Tasks_df['創建日期'] = Tasks_df['創建日期'].astype(float)
Tasks_df['創建日期'] = Tasks_df['創建日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
Tasks_df['創建日期'] = Tasks_df['創建日期'].dt.tz_convert('Asia/Taipei')
Tasks_df['創建日期'] = Tasks_df['創建日期'].dt.strftime('%Y-%m-%d')
Tasks_df['創建日期'] = pd.to_datetime(Tasks_df['創建日期'])
Tasks_df['執行狀態']= Tasks_df['執行狀態'].astype(str)
Tasks_df1 = Tasks_df.loc[Tasks_df['執行狀態'].str.contains("任務完成|接受任務")]  
Tasks_df2 = Tasks_df[Tasks_df['創建日期'] >= target_date] 

經營開發 = 經營開發[~經營開發['客戶關係連絡人'].isin(Tasks_df1['客戶關係連絡人'].drop_duplicates())]
經營開發 = 經營開發[~經營開發['客戶關係連絡人'].isin(Tasks_df2['客戶關係連絡人'].drop_duplicates())]

K_invite = pd.merge(經營開發, contact_related, on = '客戶關係連絡人', how = 'inner')
K_invite = K_invite.rename(columns={'連絡人代號_x':'連絡人代號'})
K_invite = K_invite.drop_duplicates(subset=['連絡人代號'], keep='last')

K_invite = K_invite.rename(columns={'資料區域群組名稱_x':'資料區域群組名稱','連絡人_x':'連絡人'})
K_invite = K_invite.drop(['customItem11__c'],axis=1)
K_invite.loc[(K_invite['資料區域群組名稱'].str.contains('Z')), '電訪人員'] = '謝地'

# K_invite = pd.merge(K_invite, tracking,left_on = 'name' ,right_on = '客戶關係連絡人', how = 'left')
# K_invite = K_invite.drop_duplicates(subset=['name'], keep='last')
# K_invite.to_excel("C:/Users/11020856/Desktop/jupyter/經營開發_all.xlsx",index = False) 
#手機電話去重、加回
K_invite1 = K_invite[K_invite['手機號碼'].apply(lambda x: isinstance(x, str) and len(x) == 10 and x != '0000000000' and x.startswith('09'))]
K_invite1 = K_invite1.drop_duplicates(subset=['手機號碼'], keep='last')

#K_invite2 = K_invite.drop_duplicates(subset=['連絡人代號'], keep='last')
K_invite2 = K_invite[~(K_invite['手機號碼'].isin(K_invite1['手機號碼'].drop_duplicates()))]
contact_related['關係狀態']= contact_related['關係狀態'].astype(str)
contact_related1 = contact_related[contact_related['關係狀態'].str.contains('離職')]
K_invite2 = K_invite2[~(K_invite2['客戶關係連絡人'].isin(contact_related1['客戶關係連絡人'].drop_duplicates()))]
K_invite3= pd.concat([K_invite1,K_invite2])

K_invite_經營 = K_invite3.loc[K_invite3['目標客戶類型'].str.contains("經營客戶",na=False)]
K_invite_開發 = K_invite3.loc[~K_invite3['目標客戶類型'].str.contains("經營客戶",na=False)]

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
        "xoql": '''select customItem6__c.name 客戶關係連絡人編號, customItem2__c 預約日期, customItem30__c, customItem9__c 手機號碼
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

K_visit_df['預約日期'] = pd.to_datetime(K_visit_df['預約日期'], errors='coerce')
K_visit_df = K_visit_df[pd.notna(K_visit_df['預約日期'])]
current_datetime = datetime.now()
target_date = current_datetime - timedelta(days=90)
K_visit_df['customItem30__c']= K_visit_df['customItem30__c'].astype(str)
K_visit_df = K_visit_df[K_visit_df['customItem30__c'].str.contains("是")]
K_visit_df = K_visit_df[K_visit_df['預約日期'] >= target_date]
today_date = datetime.now().strftime('%Y-%m-%d')
K_visit_df2 = K_visit_df[K_visit_df['預約日期'] >= today_date]
K_visit_df = pd.concat([K_visit_df,K_visit_df2])

K_invite_經營開發 = pd.concat([K_invite_經營,K_invite_開發])
K_invite_經營開發 = K_invite_經營開發[~K_invite_經營開發['name'].isin(K_visit_df['客戶關係連絡人編號'].drop_duplicates())]
K_invite_經營開發 = K_invite_經營開發[~K_invite_經營開發['手機號碼'].isin(K_visit_df['手機號碼'].drop_duplicates())]
#K_invite_經營.to_excel("C:/Users/11020856/Desktop/jupyter/K_invite_經營1030.xlsx",index = False)
#K_invite_開發中 = K_invite_經營開發.loc[K_invite3['目標客戶類型'].str.contains("開發中",na=False)]




#### google sheet
gc = pygsheets.authorize(service_file='/Users/wenbinyang/Downloads/analog-artifact-448608-f6-d46b6050e974.json')
spreadsheet = gc.open_by_key('1btNuZ_l_ZJyAtFBh2-CJJK3id6sc_RKdFYfOIQA6q1Q')

### 存檔
working_days_to_subtract = 1
current_date = datetime.now()
def is_weekend(date):
    return date.weekday() >= 5  # Saturday and Sunday are 5 and 6

while working_days_to_subtract > 0:
    current_date -= timedelta(days=1)
    if not is_weekend(current_date):
        working_days_to_subtract -= 1
        
yesterday  = current_date.strftime('%Y%m%d')

worksheet_name = yesterday
worksheet = spreadsheet.worksheet_by_title(worksheet_name)
oldsheet = worksheet.get_all_values()
oldsheet = pd.DataFrame(oldsheet[1:], columns=oldsheet[0])

oldsheet1 = oldsheet.loc[(oldsheet['是否邀約K大\n⭐(必填)']!='')]
formatted_yesterday = yesterday[:4] + '-' + yesterday[4:6] + '-' + yesterday[6:]
oldsheet1['邀約日期'] = formatted_yesterday

oldsheet_OLD = pd.read_excel("/Users/wenbinyang/Desktop/job/K大邀約清單(二面).xlsx", dtype='object')

###鎖表
drange = pygsheets.datarange.DataRange(worksheet=worksheet)
drange.requesting_user_can_edit = False
drange.protected = True
oldsheet_OLD = pd.concat([oldsheet1,oldsheet_OLD])
oldsheet_OLD.to_excel("/Users/wenbinyang/Desktop/job/K大邀約清單(二面).xlsx",index=False)


oldsheet_OLD = pd.read_excel("/Users/wenbinyang/Desktop/job/K大邀約清單(二面).xlsx", dtype='object')
oldsheet_OLD['邀約日期'] = pd.to_datetime(oldsheet_OLD['邀約日期'])
oldsheet_OLD = oldsheet_OLD[oldsheet_OLD['邀約日期'] >= target_date]
K_invite_經營開發 = K_invite_經營開發[~K_invite_經營開發['手機號碼'].isin(oldsheet_OLD['手機號碼'].drop_duplicates())]
K_invite_經營開發 = K_invite_經營開發.rename(columns={'accountId':'customItem11__c'})

'''
select from Tasks 
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}

#11/29開始不觸發未接
queryLocator = ''
Tasks_df = pd.DataFrame()

while True:
    data = {
        "xoql": f'''
        select customItem42__c 客戶關係連絡人, customItem8__c 執行狀態, customItem55__c 手機號碼,
        createdAt 創建日期, customItem119__c 無效電訪類型
        from customEntity14__c
        where entityType = '3028348436713387'
        and createdAt >= {timestamp}
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

Tasks_df['執行狀態']= Tasks_df['執行狀態'].astype(str)
Tasks_df['無效電訪類型']= Tasks_df['無效電訪類型'].astype(str)
Tasks_df['創建日期'] = Tasks_df['創建日期'].astype(float)
Tasks_df['創建日期'] = Tasks_df['創建日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
Tasks_df['創建日期'] = Tasks_df['創建日期'].dt.tz_convert('Asia/Taipei')
Tasks_df['創建日期'] = Tasks_df['創建日期'].dt.strftime('%Y-%m-%d')  
Tasks_df['創建日期'] = pd.to_datetime(Tasks_df['創建日期'])
Tasks_df = Tasks_df.loc[Tasks_df['執行狀態'].str.contains("任務完成|進行中")]
#Tasks_df = Tasks_df.loc[Tasks_df['執行狀態'].str.contains("等待|任務完成|進行中")]
Tasks_df1 = Tasks_df[Tasks_df['創建日期'].astype('datetime64[ns]') >= date_to_convert]

Tasks_df1 = Tasks_df1.drop_duplicates(subset=['客戶關係連絡人'], keep=False)
Tasks_df1 = Tasks_df1.drop_duplicates(subset=['手機號碼'], keep=False)
Tasks_df1 = Tasks_df1.loc[Tasks_df1['無效電訪類型'].str.contains("未接|接通")]

K_invite_經營開發['無效電訪類型'] = K_invite_經營開發['無效電訪類型'].astype(str)
K_invite_經營開發 = K_invite_經營開發.loc[~K_invite_經營開發['無效電訪類型'].str.contains("客戶|號碼|離職",na=False)]
K_invite_經營開發2 = K_invite_經營開發[~K_invite_經營開發['客戶關係連絡人'].isin(Tasks_df['客戶關係連絡人'].drop_duplicates())]
K_invite_經營開發2 = K_invite_經營開發[~K_invite_經營開發['手機號碼'].isin(Tasks_df['手機號碼'].drop_duplicates())]
K_invite_經營開發1 = K_invite_經營開發[K_invite_經營開發['客戶關係連絡人'].isin(Tasks_df1['客戶關係連絡人'].drop_duplicates())]
K_invite_經營開發2 = K_invite_經營開發2.sample(frac=1).reset_index(drop=True)

K_invite_經營開發1['customItem121__c'] = '2'
K_invite_經營開發2['customItem121__c'] = '1'

K_invite_經營開發 = pd.concat([K_invite_經營開發2,K_invite_經營開發1])

K_invite_經營開發 = K_invite_經營開發.drop_duplicates(subset=['連絡人代號'], keep='last')
K_invite_經營開發 = K_invite_經營開發.drop_duplicates(subset=['手機號碼'], keep='last')
K_invite_經營開發 = K_invite_經營開發.sort_values(by='最近聯繫時間', na_position='first') 

K_invite_經營開發['entityType'] = '3028348436713387'
# tomorrow_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
# K_invite_經營開發['customItem120__c'] = tomorrow_date
today_date = datetime.now().strftime('%Y-%m-%d')
K_invite_經營開發['customItem120__c'] = today_date
K_invite_經營開發['customItem3__c'] = 'K大邀約' + K_invite_經營開發['連絡人']
index_length = len(K_invite_經營開發)
values = [['1']] * index_length
K_invite_經營開發['customItem115__c'] = values
K_invite_經營開發['資料區域群組名稱'] = K_invite_經營開發['資料區域群組名稱'].astype(str)
K_invite_經營開發['customItem206__c'] = '9'

K_invite_經營 = K_invite_經營開發.loc[K_invite_經營開發['目標客戶類型'].str.contains("經營客戶",na=False)]
K_invite_開發 = K_invite_經營開發.loc[~K_invite_經營開發['目標客戶類型'].str.contains("經營客戶",na=False)]
K_invite_開發2 = K_invite_開發.loc[K_invite_開發['目標客戶類型'].str.contains("經營",na=False)]
K_invite_開發3 = K_invite_開發[~K_invite_開發['客戶關係連絡人'].isin(K_invite_開發2['客戶關係連絡人'].drop_duplicates())]
K_invite_開發 = pd.concat([K_invite_開發2,K_invite_開發3]) 
#K_invite_經營 = pd.concat([K_invite_經營,K_invite_開發2])
# K_invite_經營 = pd.concat([K_invite_開發2]) # 改經營加開發中
# K_invite_開發4 = K_invite_開發.loc[K_invite_開發['目標客戶類型'].str.contains("沉默",na=False)]

#K_invite_開發10 = K_invite_開發.loc[K_invite_開發['目標客戶類型'].str.contains("開發客戶",na=False)]
# K_invite_經營開發3 = Tasks_df1[Tasks_df1['手機號碼']=='0932130276']
# K_invite_經營開發3 = K_invite_經營開發1[K_invite_經營開發1['手機號碼']=='0932130276']

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
        select customItem1__c 電訪人員, customItem3__c 預估通數, customItem2__c 電訪人員類型, customItem9__c 名單類型
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
Sales_list = Sales_list[Sales_list['預估通數']!='']
Sales_list['預估通數']= Sales_list['預估通數'].astype(int)
Sales_list['名單類型']= Sales_list['名單類型'].astype(str)
Sales_list = pd.merge(Sales_list, user_df, left_on = '電訪人員', right_on = 'id', how = 'left')
Sales_list = Sales_list[Sales_list['預估通數']>0]
#Sales_list1 = Sales_list.loc[~Sales_list['電訪人員類型'].str.contains("二面",na=False)]
Sales_list1 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("產品",na=False)]
Sales_list2 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("未接",na=False)]
Sales_list3 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("二面",na=False)]


###########電訪
K_invite6 = K_invite_經營[K_invite_經營['電訪人員']!='謝地']

invited_people1 = []

for index, row in Sales_list2.iterrows():
    person = row['電訪人員']  # Assuming 'person' is the column name
    count = row['預估通數']  # Assuming 'count' is the column name

    # Extend the invited_people list with 'person' repeated 'count' times
    if not pd.isna(count):
        invited_people1.extend([person] * int(count))
invited_people1 = pd.DataFrame({'客服主任': invited_people1})

invited_people1.reset_index(drop=True, inplace=True)
K_invite6.reset_index(drop=True, inplace=True)
K_invite經營 = pd.concat([K_invite6,invited_people1],axis=1)
K_invite經營 = K_invite經營.drop(['電訪人員'],axis=1)
K_invite經營 = K_invite經營.rename(columns={'客服主任':'電訪人員'})
K_invite經營 = K_invite經營[K_invite經營['電訪人員'].notnull()]
K_invite6 = K_invite6[~K_invite6['連絡人代號'].isin(K_invite經營['連絡人代號'].drop_duplicates())]

K_invite = K_invite經營.sample(frac=1).reset_index(drop=True)
K_invite = K_invite.drop_duplicates(subset=['連絡人代號'], keep='last')
K_invite.rename(columns={'電訪人員': 'customItem10__c'}, inplace=True)
#K_invite7 = K_invite6[K_invite6['最近聯繫時間']=='2024-10-11 00:00:00']

#K_invite7 = K_invite_經營[K_invite_經營['電訪人員']!='謝地']
###########產顧
invited_people2 = []

for index, row in Sales_list1.iterrows():
    person = row['電訪人員']  # Assuming 'person' is the column name
    count = row['預估通數']  # Assuming 'count' is the column name

    # Extend the invited_people list with 'person' repeated 'count' times
    if not pd.isna(count):
        invited_people2.extend([person] * int(count))
invited_people2 = pd.DataFrame({'客服主任': invited_people2})

invited_people2.reset_index(drop=True, inplace=True)
K_invite6.reset_index(drop=True, inplace=True)
K_invite開發 = pd.concat([K_invite6,invited_people2],axis=1)
K_invite開發 = K_invite開發.drop(['電訪人員'],axis=1)
K_invite開發 = K_invite開發.rename(columns={'客服主任':'電訪人員'})
K_invite開發 = K_invite開發[K_invite開發['電訪人員'].notnull()]
K_invite6= K_invite6[~K_invite6['連絡人代號'].isin(K_invite開發['連絡人代號'].drop_duplicates())]

K_invite1 = K_invite開發.sample(frac=1).reset_index(drop=True)
K_invite1 = K_invite1.drop_duplicates(subset=['連絡人代號'], keep='last')
K_invite1.rename(columns={'電訪人員': 'customItem10__c'}, inplace=True)

K_invite10 = pd.concat([K_invite,K_invite1])
#K_invite10 = pd.concat([K_invite])
K_invite11 = K_invite10.drop_duplicates(subset=['連絡人代號'], keep='last')

K_invite11.rename(columns={'客戶關係連絡人':'customItem42__c'}, inplace=True)
K_invite_task = K_invite11[['customItem42__c','entityType','customItem120__c','customItem3__c','customItem121__c','customItem10__c','dimDepart','customItem11__c','customItem115__c','customItem206__c']]
K_invite_task = K_invite_task.astype(str)
K_invite_task['customItem10__c'].value_counts()

K_invite6.to_excel("/Users/wenbinyang/Desktop/job/last_K_invite.xlsx",index=False)



### 二面
invited_people1 = []

for index, row in Sales_list3.iterrows():
    person = row['name']  # Assuming 'person' is the column name
    count = row['預估通數']  # Assuming 'count' is the column name

    # Extend the invited_people list with 'person' repeated 'count' times
    if not pd.isna(count):
        invited_people1.extend([person] * int(count))
invited_people1 = pd.DataFrame({'客服主任': invited_people1})
invited_people1 = invited_people1.sample(frac=1).reset_index(drop=True)

invited_people1 = pd.concat([invited_people1])
invited_people1.reset_index(drop=True, inplace=True)
K_invite6.reset_index(drop=True, inplace=True)
K_invite_二面 = pd.concat([K_invite6,invited_people1],axis=1)
#K_invite_二面 = K_invite_二面[:200]
# K_invite_二面['客服主任'] = '＃郭明傑'

K_invite_二面 = K_invite_二面.rename(columns={'客服主任':'邀約人'})
K_invite_二面 = K_invite_二面[K_invite_二面['邀約人'].notnull()]
K_invite_二面.rename(columns={'資料區域群組名稱': '資料區域','公司簡稱':'公司全名','公司代號_x':'公司代號','客戶關係連絡人GS':'客戶關係聯絡人編號'}, inplace=True)
K_invite6 = K_invite6[~K_invite6['連絡人代號'].isin(K_invite_二面['連絡人代號'].drop_duplicates())]
K_invite_二面 = K_invite_二面[['邀約人','資料區域','公司代號','客戶關係聯絡人編號','公司全名','連絡人','職務類別','手機號碼','公司電話','LINEID']]

today = datetime.now()
K_invite_二面 = K_invite_二面.replace(np.nan, '', regex=True)
K_invite_二面['公司電話'] = "'" + K_invite_二面['公司電話']
K_invite_二面['手機號碼'] = "'" + K_invite_二面['手機號碼']
K_invite_二面['LINEID'] = "'" + K_invite_二面['LINEID']

new_worksheet_title = today.strftime('%Y%m%d')    ###改自動日期
new_worksheet = spreadsheet.add_worksheet(new_worksheet_title ,cols = 14, index=0)
new_worksheet.set_dataframe(pd.DataFrame(K_invite_二面), start='A1' , end='O1')
new_worksheet.frozen_rows = 1

CELL_RANGE = 'K1' 

new_worksheet.update_value(CELL_RANGE, "是否邀約K大\n⭐(必填)")

new_worksheet.set_data_validation(start ="K2", end = "K2000",
                        condition_type="ONE_OF_LIST",
                        condition_values=['成功邀約K大', '客戶拒絕邀約(開車、開會、改天)','未接(語音信箱、通話中、忙線中)',
                                          '號碼無效(空號)','號碼無效(非本人)','客戶退休/公司停業/轉職'],
                        inputMessage = "請選擇是或否", #error message
                        strict = True, #reject invalid information
                        showCustomUi = True) #drop down list

CELL_RANGE = 'L1' 

new_worksheet.update_value(CELL_RANGE, "即時通")

new_worksheet.set_data_validation(start ="L2", end = "L2000",
                        condition_type="ONE_OF_LIST",
                        condition_values=['即時通'],
                        inputMessage = "請選擇是否為即時通", #error message
                        strict = True, #reject invalid information
                        showCustomUi = True) #drop down list


CELL_RANGE = 'M1' 

new_worksheet.update_value(CELL_RANGE, "K大邀約日期時間\n⭐若成功邀約(必填)\n範例：2024-04-01 09:00(日期跟時間中間要空一格)")

CELL_RANGE = 'N1' 

new_worksheet.update_value(CELL_RANGE, "備註\n(客戶需求)\n更改電話/更改地址")


new_worksheet.apply_format("A1:I1",
                {"textFormat": {"fontSize": "12", "bold": "True"},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "backgroundColorStyle": 
                {"rgbColor":
                  {"red": 255/255, "blue": 204/255, "green": 255/255}}})
    
new_worksheet.apply_format("J1:J1",
                {"textFormat": {"fontSize": "12", "bold": "True"},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "backgroundColorStyle": 
                {"rgbColor":
                  {"red": 224/255, "blue": 0/255, "green": 0/255}}})
    
new_worksheet.apply_format("K1:M1",
                {"textFormat": {"fontSize": "12", "bold": "True"},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "backgroundColorStyle": 
                {"rgbColor":
                  {"red": 182/255, "blue": 168/255, "green": 215/255}}})
    
new_worksheet.apply_format("N1:N1",
                {"textFormat": {"fontSize": "12", "bold": "True"},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "backgroundColorStyle": 
                {"rgbColor":
                  {"red": 253/255, "blue": 102/255, "green": 217/255}}})


new_worksheet.adjust_row_height(start=2, end=2000, pixel_size=20)
new_worksheet.adjust_column_width(start=1, end=20, pixel_size=150)
new_worksheet.adjust_column_width(start=4, end=4, pixel_size=180)
new_worksheet.adjust_column_width(start=13, end=13, pixel_size=380)

drange = pygsheets.datarange.DataRange(start='A1', end='I2000', worksheet=new_worksheet)
drange.requesting_user_can_edit = False
drange.protected = True
drange.editors = ('users', ["kedingsd@gmail.com",
                            "pyhton-gsheet-insert@lithe-sonar-395510.iam.gserviceaccount.com",
                            "loren.weilun@gmail.com",
                            "kevinjyaes09@gmail.com",
                            "a5102933@gmail.com",
                            "k-229-714@onyx-sphere-376703.iam.gserviceaccount.com",
                            "strawberry52307@gmail.com"])

K_invite6.to_excel("/Users/wenbinyang/Desktop/job/last_K_invite.xlsx",index=False)



##########補名單
# '''
#   select from Tasks 
# '''
# url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
# headers = {
#     "Authorization": f"Bearer {ac_token}",
#     "Content-Type":"application/x-www-form-urlencoded"
#     # Replace with your actual access token
# }
# #改成今天日期
# date_to_convert = datetime(2024,10, 9)
# # # Calculate the Unix timestamp in milliseconds
# timestamp = int(date_to_convert.timestamp() * 1000)
# queryLocator = ''
# Tasks_df = pd.DataFrame()

# while True:
#     data = {
#         "xoql": f'''
#         select customItem42__c.name 客戶關係連絡人編號, customItem42__c.contactCode__c__c 連絡人代號, customItem8__c 執行狀態
#         from customEntity14__c
#         where entityType = '3028348436713387' and createdAt >= {timestamp}
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

# K_invite6 = K_invite_經營[~K_invite_經營['連絡人代號'].isin(Tasks_df['連絡人代號'].drop_duplicates())]   
# K_invite6 = K_invite6.astype(str)
# K_invite6 = K_invite6.loc[~K_invite6['資料區域群組名稱'].str.contains("Z")] 

# 刪二面名單
# worksheet_name = '20240328'
# worksheet = spreadsheet.worksheet_by_title(worksheet_name)
# oldsheet = worksheet.get_all_values()
# oldsheet = pd.DataFrame(oldsheet[1:], columns=oldsheet[0])

# K_invite9 = K_invite9[~K_invite9['name'].isin(oldsheet['客戶關係聯絡人編號'].drop_duplicates())]  

# K_invite10 = K_invite6[:1]
# K_invite6 = K_invite6[~K_invite6['連絡人代號'].isin(K_invite10['連絡人代號'].drop_duplicates())]
# K_invite10['電訪人員'] = '黃如苾'

# #K_invite10 = pd.read_excel("Z:/28_數據中心部/如苾/已邀約時間待定0702.xlsx", dtype='object')
# K_invite_task = pd.merge(K_invite10, user_df, left_on = '電訪人員', right_on = 'name', how = 'left')
# K_invite_task.rename(columns={'id': 'customItem10__c','客戶關係連絡人':'customItem42__c','dimDepart_y':'dimDepart'}, inplace=True)
# K_invite_task = K_invite_task[['customItem42__c','entityType','customItem120__c','customItem3__c','customItem121__c','customItem10__c','dimDepart','customItem11__c','customItem115__c','customItem206__c']]
# K_invite_task = K_invite_task.astype(str)

K_invite_task 


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
# insert to crm if dataframe over 5000
# '''
# batch_size = 5000

# # Calculate the number of batches
# num_batches = (len(K_invite_task) + batch_size - 1) // batch_size
# # Initialize a list to store responses
# all_responses = []
# # Initialize an index to keep track of the current batch
# current_batch_index = 0

# # While there are more batches to process
# while current_batch_index < num_batches:
#     start_index = current_batch_index * batch_size
#     end_index = (current_batch_index + 1) * batch_size
#     current_batch_df = K_invite_task.iloc[start_index:end_index]
#     json_data = current_batch_df.to_dict()  # 不傳遞 orient 參數
#     json_data = [dict(zip(json_data,t)) for t in zip(*json_data.values())]


#     url_2 = "https://api-p10.xiaoshouyi.com/rest/bulk/v2/batch"
#     headers = {
#         "Authorization": f"Bearer {ac_token}",
#         "Content-Type":"application/json"
#     }
#     data = {
#         "data": {
#             "jobId": f"{bulk_id}",
#             "datas": json_data
#         }
#     }
#     response = requests.post(url_2, headers=headers, json=data)
#     all_responses.append(response)
#     current_batch_index += 1
# for response in all_responses:
#     pass








# url_2 = "https://api-p10.xiaoshouyi.com/rest/bulk/v2/batch/415836869d184b024ed40428763bd6ed517e112d939e42328728071d0984f5c8/result"
# headers = {
#     "Authorization": f"Bearer {ac_token}"
# }

# response2 = requests.get(url_2, headers = headers)


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
        select id,name, customItem42__c,customItem10__c, customItem3__c, approvalStatus,entityType
        
        from customEntity14__c 
        where entityType in ('2904963933786093','3028348436713387') and createdAt >= {timestamp}
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
Tasks_df = Tasks_df.loc[Tasks_df['approvalStatus'].str.contains("待提交|撤回")] 


value_counts = Tasks_df['customItem42__c'].value_counts()
filtered_counts = value_counts[value_counts > 1]
result_df = filtered_counts.reset_index()
result_df.columns = ['customItem42__c', 'count']
print(result_df)

Tasks_df_sorted = Tasks_df.sort_values(by='entityType', ascending=True)

Tasks_df = Tasks_df_sorted.drop_duplicates(subset='customItem42__c', keep='first')

          
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
        select id,name,customItem10__c, customItem3__c,workflowStageName,approvalStatus,customItem8__c,customItem206__c
        ,customItem121__c,customItem65__c, customItem42__c.name 客戶關係連絡人編號
        from customEntity14__c
        where entityType = '3028348436713387' and createdBy = '2660038858067951' and createdAt >= {timestamp} 
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



























# import pygsheets
# import pandas as pd
# from datetime import datetime, timedelta




# # 選擇要回溯的日期範圍，例如最近 60 天
# days_back = 100
# all_data = []

# for i in range(days_back):
#     date_obj = datetime.now() - timedelta(days=i)
#     if date_obj.weekday() >= 5:  # 跳過週末
#         continue
#     sheet_title = date_obj.strftime('%Y%m%d')
#     try:
#         ws = spreadsheet.worksheet_by_title(sheet_title)
#         data = ws.get_all_values()
#         df = pd.DataFrame(data[1:], columns=data[0])
#         # 篩選已填邀約
#         df = df[df['是否邀約K大\n⭐(必填)'] != '']
#         df['邀約日期'] = date_obj.strftime('%Y-%m-%d')
#         all_data.append(df)
#     except Exception as e:
#         print(f"⚠️ 無法讀取工作表 {sheet_title}: {e}")

# # 合併所有資料
# if all_data:
#     combined_df = pd.concat(all_data, ignore_index=True)
#     print(f"✅ 讀取完成，共合併 {len(combined_df)} 筆資料")

#     # 儲存回 Excel，還原你的本地文件
#     combined_df.to_excel("/Users/wenbinyang/Desktop/job/K大邀約清單(二面).xlsx", index=False)
# else:
#     print("❗ 沒有讀取到任何資料")
