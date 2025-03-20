import pandas as pd
import requests
import numpy as np
import pyodbc
from datetime import datetime, timedelta
import pymysql

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
    "password": "Ruby5112624zAOdbVRj"
}

response = requests.post(url, data=payload)
content = response.json()
ac_token = content["access_token"]



silent_df = pd.read_excel('C:/Users/11020856/Desktop/jupyter/0719經營開發客戶標籤.xlsx')
use_account = silent_df.rename(columns={'accountCode__c': '公司代號'})
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
                          customItem342__c 郵遞區號
                   from account
                   where (customItem253__c not like 'TW-%')
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
                    customItem8__c 公司代號,
                    customItem2__c.contactName 聯絡人,
                   name AS 主要聯絡人代號,
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
            WHERE customItem37__c not LIKE 'TW%' AND customItem37__c NOT LIKE 'KDED%'
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
contact_df['employed']= contact_df['employed'].astype(str)
contact_df1 =  contact_df[contact_df['employed'].str.contains('在職', na=False)]
staff_off = pd.read_excel('C:/Users/11020856/Desktop/jupyter/海外離職人員0807.xlsx')  
first_df = pd.merge(staff_off, use_account, on = '公司代號', how = 'left')  
first_df = pd.merge(first_df, info_df, on = '公司代號', how = 'left')
first_df = pd.merge(first_df, contact_df, on = '公司代號', how = 'left')

first_df['employed'] = first_df['employed'].str.get(0)
first_df['聯絡人無效'] = first_df['聯絡人無效'].str.get(0)
first_df['倒閉'] = first_df['倒閉'].str.get(0)
first_df['職務類別'] = first_df['職務類別'].str.get(0)
wordlist = "\b(Close)|搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效拜訪|資料不全|已倒閉|C>|office duty|Support|已搬遷|inv|非目標|已結業"
first_df = first_df[~first_df['公司名稱'].str.contains(wordlist, regex=True, na=False)]
first_df = first_df[~first_df['CoShortName'].str.contains(wordlist, regex=True, na=False)]

# Create new boolean columns to indicate if the condition is met
#first_df['公司名稱_flag'] = first_df['公司名稱'].str.contains(wordlist, regex=True, na=False)
#first_df['CoShortName_flag'] = first_df['CoShortName'].str.contains(wordlist, regex=True, na=False)


#processing companyType
first_df = first_df[first_df['公司型態']!='']
first_df = first_df[~first_df['公司型態'].isna()]
first_df = first_df[first_df['公司型態'].str.contains('C|D', na=False)]
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
first_df = first_df.drop_duplicates(subset=['公司代號'])
'''
address_problem
'''
address_false = first_df[(first_df['公司地址'].str.len() < 10)]
drop_address = first_df[~first_df['公司代號'].isin(address_false['公司代號'])]
#duplicate_address = first_df[first_df.duplicated(subset=['公司地址'], keep=False)]################################
drop_address = drop_address.drop_duplicates(subset=['公司地址'], keep='first')

#first_df['drop_address<10_flag'] = first_df['公司代號'].isin(address_false['公司代號'])

# Create a boolean column indicating if the row is a duplicate based on '公司地址'
#first_df['duplicate_address_flag'] = first_df.duplicated(subset=['公司地址'], keep=False)
#drop_address =  first_df
'''
空號
'''
drop_address['公司電話'] = drop_address['公司電話'].replace('0', '')
drop_address['公司電話'] = drop_address['公司電話'].replace('-', '')
drop_address['公司電話'] = drop_address['公司電話'].replace('0-', '')
drop_address['公司電話'] = drop_address['公司電話'].replace('無', '')
drop_address['主要聯絡人手機號碼'] = drop_address['主要聯絡人手機號碼'].replace('0', '')
drop_address.fillna('', inplace=True)
#drop_address.loc[~drop_address['主要聯絡人手機號碼'].fillna('').str.startswith('09'), '主要聯絡人手機號碼'] = ''
two_no_phone = drop_address[(drop_address['公司電話'].str.contains('空號') & (drop_address['主要聯絡人手機號碼'] == ''))]
two_no_phone2 = drop_address[((drop_address['公司電話'] == '') & (drop_address['主要聯絡人手機號碼'] == ''))]
no_phone_final = pd.concat([two_no_phone, two_no_phone2])
no_phone_name = drop_address[drop_address['主要聯絡人'].str.contains('空號')]
no_phone_final = pd.concat([no_phone_final, no_phone_name])###########################################
drop_phone = drop_address[~drop_address['公司代號'].isin(no_phone_final['公司代號'])]

#first_df['drop_no_phone_flag'] = drop_address['公司代號'].isin(no_phone_final['公司代號'])

'''
重複電話號碼
'''
blank = drop_phone[drop_phone['主要聯絡人手機號碼']=='']
other = drop_phone[~drop_phone['公司代號'].isin(blank['公司代號'])]
duplicate_mobile = other[other.duplicated(subset=['主要聯絡人手機號碼'], keep=False)]
other = other.drop_duplicates(subset=['主要聯絡人手機號碼'], keep='first')
drop_mobile = pd.concat([blank, other])
blank = drop_mobile[drop_mobile['公司電話']=='']
other = drop_mobile[~drop_mobile['公司代號'].isin(blank['公司代號'])]
duplicate_office = other[other.duplicated(subset=['公司電話'], keep=False)]
other = other.drop_duplicates(subset=['公司電話'], keep='first')
drop_office = pd.concat([blank, other])

# blank = first_df[first_df['主要聯絡人手機號碼']=='']
# other = first_df[first_df['公司代號'].isin(blank['公司代號'])]
# duplicate_mobile = other[other.duplicated(subset=['主要聯絡人手機號碼'], keep=False)]
# other = other.drop_duplicates(subset=['主要聯絡人手機號碼'], keep='first')
# drop_mobile = pd.concat([blank, other])
# blank = drop_mobile[drop_mobile['公司電話']=='']
# other = drop_mobile[drop_mobile['公司代號'].isin(blank['公司代號'])]
# duplicate_office = other[other.duplicated(subset=['公司電話'], keep=False)]
# other = other.drop_duplicates(subset=['公司電話'], keep='first')
# drop_office = pd.concat([blank, other])

# first_df['drop_duplicate_phone_flag'] = first_df['公司代號'].isin(drop_office['公司代號'])

'''
同一主要聯絡人代號(無主要聯絡人)
'''
duplicate_contact = drop_office[drop_office.duplicated(subset=['主要聯絡人代號'], keep=False)]
drop_contact = drop_office.drop_duplicates(subset=['主要聯絡人代號'], keep='first')
drop_contact = drop_contact[drop_contact['主要聯絡人代號']!='']

# duplicate_contact = first_df[first_df.duplicated(subset=['主要聯絡人代號'], keep=False)]
# drop_contact = first_df.drop_duplicates(subset=['主要聯絡人代號'], keep='first')
# drop_contact = first_df[first_df['主要聯絡人代號']!='']

# first_df['drop_duplicate_contact_flag'] = first_df['公司代號'].isin(drop_contact['公司代號'])
'''
勿寄型錄
'''
drop_contact['聯絡人勿擾選項'] = drop_contact['聯絡人勿擾選項'].astype(str)
contact = drop_contact[drop_contact['聯絡人勿擾選項'].str.contains('型錄')]
drop_contact['公司勿擾選項'] = drop_contact['公司勿擾選項'].astype(str)
company = drop_contact[drop_contact['公司勿擾選項'].str.contains('型錄')]
no_book = pd.concat([contact, company], ignore_index=True)
no_book_final = no_book.drop_duplicates(subset=['公司代號'], keep='first')###################################################
drop_book = drop_contact[~drop_contact['公司代號'].isin(no_book_final['公司代號'])]

# first_df['聯絡人勿擾選項'] =first_df['聯絡人勿擾選項'].astype(str)
# contact = first_df[first_df['聯絡人勿擾選項'].str.contains('型錄')]
# first_df['公司勿擾選項'] = first_df['公司勿擾選項'].astype(str)
# company = first_df[first_df['公司勿擾選項'].str.contains('型錄')]
# no_book = pd.concat([contact, company], ignore_index=True)
# no_book_final = no_book.drop_duplicates(subset=['公司代號'], keep='first')###################################################
# drop_book = drop_contact[drop_contact['公司代號'].isin(no_book_final['公司代號'])]
# first_df['drop_book_flag'] = first_df['公司代號'].isin(drop_book['公司代號'])

'''
聯絡人離職
'''
not_main = drop_book[drop_book['employed']=='在職（配合）']
leave = drop_book[drop_book['employed']=='離職']
leave2 = drop_book[drop_book['主要聯絡人'].str.contains('離職')]
leave = pd.concat([leave, leave2])
stay_df = drop_book[~drop_book['公司代號'].isin(not_main['公司代號']) & ~drop_book['公司代號'].isin(leave['公司代號'])]

# not_main = first_df[first_df['employed']=='在職（配合）']
# leave = first_df[first_df['employed']=='離職']
# leave2 = first_df[first_df['主要聯絡人'].str.contains('離職')]
# leave = pd.concat([leave, leave2])
# stay_df = first_df[first_df['公司代號'].isin(not_main['公司代號']) & ~drop_book['公司代號'].isin(leave['公司代號'])]
# first_df['drop_not_main_flag'] = first_df['公司代號'].isin(stay_df['公司代號'])


'''
聯絡人無效
'''
wrong_contact = stay_df[stay_df['聯絡人無效']!='否']
wrong_contact2 = stay_df[stay_df['主要聯絡人'].str.contains('電話')]
wrong_contact = pd.concat([wrong_contact, wrong_contact2])
contact_safe = stay_df[~stay_df['公司代號'].isin(wrong_contact['公司代號'])]

# wrong_contact = first_df[first_df['聯絡人無效']!='否']
# wrong_contact2 = first_df[first_df['主要聯絡人'].str.contains('電話')]
# wrong_contact = pd.concat([wrong_contact, wrong_contact2])
# contact_safe = first_df[first_df['公司代號'].isin(wrong_contact['公司代號'])]
# first_df['drop_wrong_contact_flag'] = first_df['公司代號'].isin(contact_safe['公司代號'])


'''
停機空號倒閉
'''
not_work = contact_safe[(contact_safe['空號'] != '0') | (contact_safe['停機'] != '0')]
contact_safe = contact_safe[(contact_safe['空號'] == '0') & (contact_safe['停機'] == '0')]
bankrupt = contact_safe[contact_safe['倒閉']=='是']
contact_safe = contact_safe[~contact_safe['公司代號'].isin(bankrupt['公司代號'])]

# not_work = first_df[(first_df['空號'] != '0') | (first_df['停機'] != '0')]
# contact_safe = first_df[(first_df['空號'] == '0') & (first_df['停機'] == '0')]
# bankrupt = contact_safe[contact_safe['倒閉']=='是']
# contact_safe = first_df[first_df['公司代號'].isin(bankrupt['公司代號'])]
# first_df['drop_not_work_flag'] = first_df['公司代號'].isin(contact_safe['公司代號'])
'''
管制客戶
'''
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
df2.columns = ["SAP代號", "SAP信用管制"]
contact_safe = pd.merge(contact_safe, df2 ,on ='SAP代號',how='left')
contact_safe = contact_safe[contact_safe['SAP信用管制'] != '管制']
#first_df['drop_管制_flag'] = first_df['公司代號'].isin(contact_safe['公司代號'])


'''
退休/轉行/號碼錯誤/非本人
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
date_to_convert = datetime(2023,11, 27)
timestamp = int(date_to_convert.timestamp() * 1000)


queryLocator = ''
tracking = pd.DataFrame()
while True:
    data = {
        "xoql": f'''
        select
        customItem48__c.name 客戶關係連絡人,
        customItem177__c 無效電訪類型

        from customEntity15__c
        where customItem40__c >= {timestamp}
        and customItem118__c not like '%TW%' 
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

tracking['無效電訪類型']= tracking['無效電訪類型'].astype(str)
tracking1 = tracking.loc[tracking['無效電訪類型'].str.contains('無效|客戶退休|號碼', na=False)]
contact_safe = contact_safe[~contact_safe['主要聯絡人代號'].isin(tracking1['客戶關係連絡人'])]
#first_df['drop_無效_flag'] = first_df['公司代號'].isin(contact_safe['公司代號'])


'''
關聯客戶
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


cursor.execute("SELECT COMPANYID,RELATED_FINAL FROM related_company ")

results = cursor.fetchall()
related_company = pd.DataFrame(results)
related_company.columns = ["公司代號", "RELATED_FINAL"]
contact_safe = pd.merge(contact_safe, related_company ,on ='公司代號',how='left')
contact_safe['公司代號'] = contact_safe['RELATED_FINAL']
contact_safe = contact_safe.drop_duplicates(subset=['公司代號'], keep='first')

#first_df['drop_關聯客戶_flag'] = contact_safe.duplicated(subset=['公司地址'], keep=False)
'''
沉默客戶(僅新馬港)
'''
contact_SGMYHK = contact_safe.loc[~contact_safe['區域'].str.contains('SG|MY|HK', na=False)]
contact_safe = contact_safe[~contact_safe['主要聯絡人代號'].isin(contact_SGMYHK['主要聯絡人代號'])]
contact_SGMYHK = contact_SGMYHK.loc[~contact_SGMYHK['mark'].str.contains('沉默客戶', na=False)]
contact_final = pd.concat([contact_safe, contact_SGMYHK])
contact_final = contact_final[contact_final['mark']!='']
contact_final = contact_final.loc[contact_final['區域'].str.contains('SG|MY|HK|IN|PH|ID|VN|TH', na=False)]

# contact_SGMYHK = first_df.loc[~first_df['區域'].str.contains('SG|MY|HK', na=False)]
# contact_safe = first_df[~first_df['主要聯絡人代號'].isin(contact_SGMYHK['主要聯絡人代號'])]
# contact_SGMYHK = contact_SGMYHK.loc[~contact_SGMYHK['mark'].str.contains('沉默客戶', na=False)]
# contact_final = pd.concat([contact_safe, contact_SGMYHK])
# contact_final = contact_final[contact_final['mark']!='']
# first_df['drop_沉默客戶_flag'] = first_df['公司代號'].isin(contact_final['公司代號'])
# first_df = first_df.loc[first_df['區域'].str.contains('SG|MY|HK|IN|PH|ID|VN|TH', na=False)]


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

date_to_convert = datetime(2024,1, 1)
# # Calculate the Unix timestamp in milliseconds
timestamp = int(date_to_convert.timestamp() * 1000)

while True:
    data = {
        "xoql": f'''select catalogGiftSendRequest__c.name 型錄發放申請編號, gift__c.name 申請物品, accountCode__c 公司代號, customItem15__c 區域
         from customEntity28__c
        where createdAt >= {timestamp} and customItem15__c not like '%TW%' 
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
    
gift_df = gift_df.loc[gift_df['申請物品'].str.contains('Eco', na=False)]
#contact_final = contact_final[~contact_final['公司代號'].isin(gift_df['公司代號'])]

'''
select from 發放申請
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
gift_df1 = pd.DataFrame()

date_to_convert = datetime(2024,1, 1)
# # Calculate the Unix timestamp in milliseconds
timestamp = int(date_to_convert.timestamp() * 1000)

while True:
    data = {
        "xoql": f'''select name 型錄發放申請編號, customItem97__c 申請物品, accountCode__c 公司代號, customItem29__c 區域
         from customEntity25__c
        where createdAt >= {timestamp} and customItem29__c not like '%TW%' 
        ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    gift_df1 = pd.concat([gift_df1, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
    
gift_df1 = gift_df1.loc[gift_df1['申請物品'].str.contains('Eco', na=False)]

gift_df1 = pd.concat([gift_df, gift_df1])
contact_final = contact_final[~contact_final['公司代號'].isin(gift_df1['公司代號'])]

#first_df['drop_Eco_flag'] = first_df1['公司代號'].isin(contact_final['公司代號'])

first_df = pd.read_excel('C:/Users/11020856/Desktop/jupyter/海外離職人員08081.xlsx') 
first_df1 = pd.read_excel('C:/Users/11020856/Desktop/jupyter/海外有效型錄0729.xlsx') 

first_df = drop_phone[~drop_phone['公司代號'].isin(gift_df1['公司代號'])]
first_df = first_df[(first_df['公司地址'].str.len() > 10)]
first_df1 = first_df[~first_df['公司代號'].isin(first_df1['公司代號'])]
first_df1 = first_df1.drop_duplicates(subset=['主要聯絡人代號_y'], keep='first')
first_df1 = first_df1.drop_duplicates(subset=['手機號碼'], keep='first')
first_df1['employed'].replace('', np.nan, inplace=True)
filtered_df = first_df1.groupby('公司代號').filter(lambda x: (x['employed'].isin(['離職', ''])).all())
filtered_df = filtered_df[filtered_df.duplicated(subset=['公司地址'], keep=False)]
filtered_df = filtered_df[~filtered_df['公司代號'].isin(contact_df1['公司代號'])]
filtered_df.to_excel("C:/Users/11020856/Desktop/jupyter/海外型錄聯絡人均離職0808.xlsx",index = False) 
