import pandas as pd
import pyodbc
import json
import requests
from datetime import datetime, timedelta
import pymysql
import numpy as np

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
find needed account info
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
info_df = pd.DataFrame()
 
while True:
    data = {
        "xoql": '''
                   select accountCode__c as 公司代號
                   from account
                   where (customItem253__c like 'TW-%' and customItem253__c not like 'TW-Z%' and customItem253__c not like '%INV%')
                ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    info_df = pd.concat([info_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
    
# '''
# 關聯客戶
# '''
# connection = pymysql.connect(
#         host='192.168.1.253',  # 数据库地址
#         port=3307,
#         user='DATeam',          # 用户名
#         password='Dateam@1234', # 密码
#         database='db01',        # 数据库名称
#         charset='utf8'       # 字符编码
#     )
# cursor = connection.cursor()


# cursor.execute("SELECT COMPANYID,RELATED_FINAL FROM related_company ")

# results = cursor.fetchall()
# related_company = pd.DataFrame(results)
# related_company.columns = ["公司代號", "RELATED_FINAL"]
# first_df = pd.merge(info_df, related_company ,on ='公司代號',how='left')
# first_df['公司代號'] = first_df['RELATED_FINAL']
# first_df = first_df.drop_duplicates(subset=['公司代號'], keep='first')
first_df = info_df.drop_duplicates(subset=['公司代號'], keep='first')

'''
find needed account info
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
info_df1 = pd.DataFrame()
 
while True:
    data = {
        "xoql": '''
                   select accountCode__c as 公司代號,
                          Phone__c as 公司電話,
                          customItem327__c as 主要聯絡人, 
                          customItem328__c as 主要聯絡人手機號碼, 
                          customItem326__c.name as 主要聯絡人代號,
                          SAP_CompanyID__c as SAP代號,
                          customItem202__c as 公司地址,
                          customItem291__c as 公司勿擾選項,
                          customItem326__c.customItem36__c as 聯絡人勿擾選項,
                          customItem253__c as 區域,
                          customItem199__c as 公司型態,
                          customItem345__c as 最近聯絡日期, 
                          customItem226__c as 建檔日期,
                          customItem322__c 目標客戶類型,
                          approvalStatus 審核狀態
                          
                   from account
                   where (customItem253__c like 'TW-%' and customItem253__c not like 'TW-Z%' and customItem253__c not like '%INV%')
                   and (customItem199__c LIKE '%C%' OR customItem199__c LIKE '%D%')
                ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    info_df1 = pd.concat([info_df1, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']

info_df1['審核狀態']= info_df1['審核狀態'].astype(str)
info_df1['審核狀態_flag'] = info_df1['審核狀態'].str.contains('Approved', regex=True, na=False)
first_df = pd.merge(first_df, info_df1 ,on ='公司代號',how='left')
#first_df1 = pd.merge(first_df, sap1, on = 'SAP代號', how = 'inner') 
first_df['目標客戶類型'] = first_df['目標客戶類型'].astype(str)
first_df = first_df.loc[first_df['目標客戶類型'].str.contains("經營",na = False)]
'''
find DM address
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
DM_df = pd.DataFrame()
 
while True:
    data = {
        "xoql": '''
                   select customItem12__c  公司代號,
                          customItem10__c 地址分類,
                          nation__c 國家,
                          customItem9__c 型錄地址
                   from customEntity9__c
                ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    DM_df = pd.concat([DM_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
  
DM_df['地址分類'] = DM_df['地址分類'].astype(str)
DM_df['國家'] = DM_df['國家'].astype(str)    
DM_df = DM_df.loc[DM_df['地址分類'].str.contains("型錄",na = False)]
DM_df = DM_df.loc[DM_df['國家'].str.contains("台灣",na = False)]
DM_df = DM_df[['公司代號','型錄地址']]
first_df = pd.merge(first_df, DM_df ,on ='公司代號',how='left')
first_df = first_df[first_df['審核狀態_flag'].notna()]
first_df2 = first_df.drop_duplicates(subset=['公司代號'], keep='first')
first_df3 = first_df[~first_df['公司代號'].isin(first_df2['公司代號'])]

'''
more related_contact info
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
contact_df = pd.DataFrame()
 
while True:
    data = {
        "xoql": '''
            SELECT 
                   name AS 聯絡人代號,
                   customItem2__c.contactName 連絡人,
                   customItem8__c 公司代號,
                   contactPhone__c__c 手機號碼,
                   customItem24__c AS employed,
                   customItem38__c AS 拜電訪標籤,
                   customItem42__c AS 聯絡人無效,
                   customItem83__c AS 無效電訪,
                   customItem95__c AS 職務類別,
                   customItem1__c.accountName as 公司名稱,
                   customItem9__c AS CoShortName,
                   customItem1__c.customItem278__c AS 倒閉,
                   customItem50__c as 空號,
                   customItem51__c as 停機,
                   customItem52__c as 號碼錯誤非本人
                   
            FROM customEntity22__c
            WHERE customItem37__c LIKE 'TW%' and (customItem5__c like '%C%' or customItem5__c like '%D%')
            ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    contact_df = pd.concat([contact_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
    

first_df = pd.merge(first_df, contact_df, on = '公司代號', how = 'inner')
#first_df['employed'] = first_df['employed'].str.get(0)
first_df['聯絡人無效'] = first_df['聯絡人無效'].str.get(0)
first_df = first_df.loc[~first_df['倒閉'].str.contains("是",na = False)]
first_df['職務類別'] = first_df['職務類別'].str.get(0)
wordlist = "\b(Close)|搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|已倒閉|C>|office duty|Support|inv|非目標|已結業|管制|非營業"
wordlist1 = "空機|停話|號碼錯誤|非本人|解雇|非單位成員|勿電訪|停用"
#first_df['連絡人_flag'] = first_df['連絡人'].str.contains(wordlist1, regex=True, na=False)
# first_df = first_df[~first_df['公司名稱'].str.contains(wordlist, regex=True, na=False)]
# first_df = first_df[~first_df['CoShortName'].str.contains(wordlist, regex=True, na=False)]
# first_df = first_df[~first_df['連絡人'].str.contains(wordlist1, regex=True, na=False)]

first_df['公司名稱_flag'] = first_df['公司名稱'].str.contains(wordlist, regex=True, na=False)
first_df['CoShortName_flag'] = first_df['CoShortName'].str.contains(wordlist, regex=True, na=False)
first_df['公司倒閉_flag'] = first_df['倒閉'].str.contains('是', regex=True, na=False)
first_df['連絡人_flag'] = first_df['連絡人'].str.contains(wordlist1, regex=True, na=False)
#processing companyType

first_df['最近聯絡日期'] = pd.to_numeric(first_df ['最近聯絡日期'], errors='coerce') 
first_df['最近聯絡日期'] = first_df['最近聯絡日期'].astype(float)
first_df['最近聯絡日期'] = first_df['最近聯絡日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
first_df['最近聯絡日期'] = first_df['最近聯絡日期'].dt.tz_convert('Asia/Taipei')
first_df['最近聯絡日期'] = first_df['最近聯絡日期'].dt.strftime('%Y-%m-%d %H:%M:%S')
first_df['建檔日期'] = pd.to_numeric(first_df['建檔日期'], errors='coerce') 
first_df['建檔日期'] = first_df['建檔日期'].astype(float)
first_df['建檔日期'] = first_df['建檔日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
first_df['建檔日期'] = first_df['建檔日期'].dt.tz_convert('Asia/Taipei')
first_df['建檔日期'] = first_df['建檔日期'].dt.strftime('%Y-%m-%d %H:%M:%S')
first_df = first_df.sort_values(by=['最近聯絡日期', '建檔日期'], ascending=False)
first_df2 = first_df.drop_duplicates(subset=['公司代號'])


'''
address_problem
'''
address_false = first_df[(first_df['公司地址'].str.len() < 6)]
drop_address = first_df[~first_df['公司代號'].isin(address_false['公司代號'])]
duplicate_address = first_df[first_df.duplicated(subset=['公司地址'], keep=False)]################################
drop_address = drop_address.drop_duplicates(subset=['公司地址'], keep='first')

#first_df = pd.read_excel("C:/Users/11020856/Desktop/jupyter/地址更新1.xlsx", dtype='object')
#contact_df.to_excel("C:/Users/11020856/Desktop/jupyter/連絡人代號.xlsx",index = False)
#first_df['公司地址'] = first_df['公司地址'].str.replace(r'^\d{3}', '', regex=True)
first_df.loc[first_df['型錄地址'].notna(), '公司地址'] = first_df['型錄地址']
first_df = first_df[(first_df['公司地址'].str.len() > 6)]
first_df['公司地址重複_flag'] = first_df.duplicated(subset=['公司地址'], keep=False)

'''
勿寄型錄
'''
#drop_contact['聯絡人勿擾選項'] = drop_contact['聯絡人勿擾選項'].astype(str)
#contact = drop_contact[drop_contact['聯絡人勿擾選項'].str.contains('型錄|電訪')]
drop_address['公司勿擾選項'] = drop_address['公司勿擾選項'].astype(str)
no_book = drop_address[drop_address['公司勿擾選項'].str.contains('型錄|電訪')]
no_book = pd.concat( [no_book], ignore_index=True)
no_book_final = no_book.drop_duplicates(subset=['公司代號'], keep='first')###################################################
drop_book = drop_address[~drop_address['公司代號'].isin(no_book_final['公司代號'])]


#first_df['公司勿擾選項'] = first_df['公司勿擾選項'].astype(str)
#first_df = first_df.loc[~first_df['公司勿擾選項'].str.contains("型錄|電訪",na = False)]

first_df['公司勿擾選項'] = first_df['公司勿擾選項'].astype(str)
first_df['公司勿電訪型錄_flag'] = first_df['公司勿擾選項'].str.contains('型錄|電訪', regex=True, na=False)
'''
停機空號倒閉
'''
# #not_work = contact_safe[(contact_safe['空號'] != '0') | (contact_safe['停機'] != '0')]
# contact_safe = drop_book[(drop_book['空號'] == '0') & (drop_book['停機'] == '0')]
# bankrupt = contact_safe[contact_safe['倒閉']=='是']
# contact_safe = contact_safe[~contact_safe['公司代號'].isin(bankrupt['公司代號'])]
'''
管制客戶
'''
##清除公司無效
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
df2.columns = ["SAP代號", "SAP信用管制"]
first_df = pd.merge(first_df, df2 ,on ='SAP代號',how='left')
first_df = first_df[first_df['SAP信用管制'] != '管制']

first_df['公司管制_flag'] = first_df['SAP信用管制'].str.contains('管制', regex=True, na=False)


#first_df['關聯客戶_flag'] = first_df.duplicated(subset=['公司代號'], keep=False)



'''
空號
'''
first_df['公司電話'] = first_df['公司電話'].replace('0', '')
first_df['公司電話'] = first_df['公司電話'].replace('-', '')
first_df['公司電話'] = first_df['公司電話'].replace('0-', '')
first_df['公司電話'] = first_df['公司電話'].replace('無', '')
first_df['手機號碼'] = first_df['手機號碼'].replace('0', '')
first_df.fillna('', inplace=True)
#first_df.loc[~first_df['主要聯絡人手機號碼'].fillna('').str.startswith('09'), '主要聯絡人手機號碼'] = ''
# two_no_phone = first_df[((first_df['公司電話'] == '') & (first_df['手機號碼'] == ''))]
# first_df = first_df[~first_df['公司代號'].isin(two_no_phone['公司代號'])]

first_df['無電話號碼'] = (first_df['公司電話'] == '') & (first_df['手機號碼'] == '')



'''
重複電話號碼
'''
# blank = drop_phone[drop_phone['主要聯絡人手機號碼']=='']
# other = drop_phone[~drop_phone['公司代號'].isin(blank['公司代號'])]
# duplicate_mobile = other[other.duplicated(subset=['主要聯絡人手機號碼'], keep=False)]
# other = other.drop_duplicates(subset=['主要聯絡人手機號碼'], keep='first')
# drop_mobile = pd.concat([blank, other])
# blank = drop_mobile[drop_mobile['公司電話']=='']
# other = drop_mobile[~drop_mobile['公司代號'].isin(blank['公司代號'])]
# duplicate_office = other[other.duplicated(subset=['公司電話'], keep=False)]
# other = other.drop_duplicates(subset=['公司電話'], keep='first')
# drop_office = pd.concat([blank, other])
first_df['公司電話重複'] = first_df.duplicated(subset=['公司電話'], keep=False)
first_df['聯絡人電話重複'] = first_df.duplicated(subset=['手機號碼'], keep=False)

'''
同一主要聯絡人代號(無主要聯絡人)
'''
# duplicate_contact = drop_office[drop_office.duplicated(subset=['主要聯絡人代號'], keep=False)]
# drop_contact = drop_office.drop_duplicates(subset=['主要聯絡人代號'], keep='first')
# drop_contact = drop_contact[drop_contact['主要聯絡人代號']!='']
first_df['無主要聯絡人代號'] = (first_df['主要聯絡人代號'] == '') 


'''
聯絡人離職
'''
# not_main = drop_contact[drop_contact['employed']=='在職（配合）']
# leave = drop_contact[drop_contact['employed']=='離職']
# leave2 = drop_contact[drop_contact['主要聯絡人'].str.contains('離職')]
# leave = pd.concat([leave, leave2])
# stay_df = drop_contact[~drop_contact['公司代號'].isin(not_main['公司代號']) & ~drop_contact['公司代號'].isin(leave['公司代號'])]
first_df['employed'] = first_df['employed'].astype(str)
# first_df = first_df.loc[first_df['employed'].str.contains("主要",na = False)]
first_df['主要聯絡人非主要'] = first_df['employed'].str.contains('離職|配合', regex=True, na=False)
'''
聯絡人無效
'''
# wrong_contact = stay_df[stay_df['聯絡人無效']!='否']
# wrong_contact2 = stay_df[stay_df['主要聯絡人'].str.contains('電話')]
# wrong_contact = pd.concat([wrong_contact, wrong_contact2])
# contact_safe2 = stay_df[~stay_df['公司代號'].isin(wrong_contact['公司代號'])]

first_df['聯絡人無效'] = first_df['聯絡人無效'].astype(str)
#first_df = first_df.loc[~first_df['聯絡人無效'].str.contains("是",na = False)]
first_df['聯絡人無效_flag'] = first_df['聯絡人無效'].str.contains('是', regex=True, na=False)

'''
聯絡人勿寄型錄
'''
first_df['聯絡人勿擾選項'] = first_df['聯絡人勿擾選項'].astype(str)
#first_df = first_df.loc[~first_df['聯絡人勿擾選項'].str.contains("型錄|電訪",na = False)]
first_df['聯絡人勿電訪型錄_flag'] = first_df['聯絡人勿擾選項'].str.contains('型錄|電訪', regex=True, na=False)

'''
職務類別排序
'''
#first_df1 = first_df[(first_df['無主要聯絡人代號']==True)|(first_df['主要聯絡人非主要']==True)|(first_df['聯絡人勿電訪型錄_flag']==True)|(first_df['聯絡人無效_flag']==True)|first_df['主要聯絡人無效_flag']==True]
contact_safe2 = first_df
contact_safe2.loc[contact_safe2['職務類別'].str.contains('015',na=False), 'order'] = '1'
contact_safe2.loc[contact_safe2['職務類別'].str.contains('004',na=False), 'order'] = '2'
contact_safe2.loc[contact_safe2['職務類別'].str.contains('005',na=False), 'order'] = '3'
contact_safe2.loc[contact_safe2['職務類別'].str.contains('006',na=False), 'order'] = '4'
contact_safe2.loc[contact_safe2['主要聯絡人手機號碼']==contact_safe2['手機號碼'], 'order'] = '5'

contact_safe2 = contact_safe2.sort_values(by='order')
contact_safe2 = contact_safe2.groupby('公司代號').head(1)

contact_safe2['目標客戶類型'] = contact_safe2['目標客戶類型'].astype(str)
contact_safe2 = contact_safe2.loc[contact_safe2['目標客戶類型'].str.contains("經營",na = False)]
contact_safe2['公司地址重複_flag'] = contact_safe2.duplicated(subset=['公司地址'], keep=False)
contact_safe2['公司電話重複'] = contact_safe2.duplicated(subset=['公司電話'], keep=False)
contact_safe2['聯絡人電話重複'] = contact_safe2.duplicated(subset=['手機號碼'], keep=False)


# first_df2 = contact_safe2[contact_safe2['公司代號'].isin(first_df1['公司代號'])]
# first_df3 = first_df[~first_df['公司代號'].isin(first_df1['公司代號'])]
# first_df4 = first_df1[~first_df1['公司代號'].isin(first_df2['公司代號'])]


# first_df2 = first_df2.sort_values(by='order')
# first_df = pd.concat([first_df2,first_df3])

#first_df = pd.read_excel("C:/Users/11020856/Desktop/jupyter/CD類超耐磨型錄明細0103.xlsx", dtype='object')

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


while True:
    data = {
        "xoql": '''
        select customItem48__c 公司代號,gift__c.name 物品名稱
        from customEntity28__c
        where  customItem49__c like 'TW%'
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

gift_df['物品名稱'] = gift_df['物品名稱'].astype(str)
gift_df = gift_df.loc[gift_df['物品名稱'].str.contains("超耐磨木地板型錄",na = False)]

#contact_safe2 = contact_safe2[~contact_safe2['公司代號'].isin(gift_df['公司代號'])]

contact_safe2['超耐磨木地板型錄'] = contact_safe2['公司代號'].isin(gift_df['公司代號'])
# contact_safe2 = pd.merge(contact_safe2, sap1 ,left_on ='SAP代號',right_on ='買方',how='inner')
# contact_safe3 = contact_safe1[~contact_safe1['公司代號'].isin(contact_safe2['公司代號'])]
# contact_safe1[contact_safe1['主要聯絡人手機號碼']=='0937450299']

# '''
# from 銷貨
# '''
# connection = pymysql.connect(
#         host='192.168.1.253',  # 数据库地址
#         port=3307,
#         user='DATeam',          # 用户名
#         password='Dateam@1234', # 密码
#         database='db01',        # 数据库名称
#         charset='utf8'       # 字符编码
#     )
# cursor = connection.cursor()

# cursor.execute("""SELECT 買方,未稅本位幣, 物料, 物料群組  FROM sap_sales_data 
#                 where 買方 like 'TW%' and 預計發貨日期 >='2024/01/22' 
#                 """)

# results = cursor.fetchall()
# sap = pd.DataFrame(results)
# sap.columns = ["SAP代號", "未稅本位幣","物料","物料群組" ]
# #sap1 = sap.drop_duplicates(subset=['買方'])
# sap1 = sap.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '近1年交易金額')
# contact_safe3  = pd.merge(contact_safe2, sap1 ,on ='SAP代號',how='left')

# cursor.execute("""SELECT 買方,未稅本位幣, 物料, 物料群組  FROM sap_sales_data 
#                 where 買方 like 'TW%' and 預計發貨日期 >='2021/01/22' 
#                 """)

# results = cursor.fetchall()
# sap = pd.DataFrame(results)
# sap.columns = ["SAP代號", "未稅本位幣","物料","物料群組" ]
# #sap1 = sap.drop_duplicates(subset=['買方'])
# sap1 = sap.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '近3年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap1 ,on ='SAP代號',how='left')
# sap2 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith(('K3', 'KE', 'WE')))]
# sap3 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith('WX1')) &
#     sap['物料'].apply(lambda x: isinstance(x, str) and x.startswith('K3'))]
# sap2 = pd.concat([sap2, sap3])
# sap2 = sap2.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '木地板近三年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap2 ,on ='SAP代號',how='left')

# sap3 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith(('WF', 'F', 'KEF')))]
# sap3 = sap3.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '環保地板近三年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap3 ,on ='SAP代號',how='left')

# sap4 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith(('FM', 'KEM', 'WM')))]
# sap4 = sap4.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '超耐磨木地板近三年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap4 ,on ='SAP代號',how='left')


# contact_safe1.to_excel("C:/Users/11020856/Desktop/jupyter/CD類超耐磨型錄明細1107.xlsx",index = False)
# contact_safe2 = pd.merge(contact_safe, sap1 ,left_on ='SAP代號',right_on ='買方',how='inner')
contact_safe2.to_excel("C:/Users/11020856/Desktop/jupyter/CD類超耐磨型錄明細0121.xlsx",index = False)
first_df3.to_excel("C:/Users/11020856/Desktop/jupyter/關聯客戶無聯絡人0107.xlsx",index = False)