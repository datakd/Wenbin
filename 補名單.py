
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

K_invite6 = pd.read_excel("/Users/wenbinyang/Desktop/job/last_K_invite.xlsx")

today = datetime.now()

target_sales = {
    '葉子瑄': 140
    # ,'黃譯萱': 100
    # ,'吳承遠': 140
    # ,'黃紹誠': 140
    # ,'劉冠宏': 140
    # ,'李瑜瑩': 80
    # ,'張淇雁': 80
    # ,'賴盈豪': 60
}


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
        select customItem1__c 電訪人員, customItem3__c 預估通數, customItem2__c 電訪人員類型, 
        customItem9__c 名單類型,
        customItem1__c.name as name, customItem1__c.dimDepart as dimDepart
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
Sales_list = Sales_list[Sales_list['預估通數']>0]

Sales_list = Sales_list[Sales_list['name'].str.extract(r'(' + '|'.join(target_sales.keys()) + ')')[0].notnull()].copy()

Sales_list['預估通數'] = Sales_list['name'].apply(
    lambda name: next((num for key, num in target_sales.items() if key in name), None)
)



#Sales_list1 = Sales_list.loc[~Sales_list['電訪人員類型'].str.contains("二面",na=False)]
Sales_list1 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("產品",na=False)]
Sales_list2 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("未接",na=False)]
Sales_list3 = Sales_list.loc[Sales_list['電訪人員類型'].str.contains("二面",na=False)]






###########電訪
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

