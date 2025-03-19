
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


K_invite6 = pd.read_excel("/Users/wenbinyang/Desktop/job/last_K_invite.xlsx")