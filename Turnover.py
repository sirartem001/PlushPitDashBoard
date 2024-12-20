import requests
import datetime as dt
import pandas as pd

API_URL = 'https://api-seller.ozon.ru'
API_KEY = '22c5ec5b-5e3f-4002-afca-95fbbdae08aa'
CLIENT_ID = '1997569'

headers = {
    'Client-Id': CLIENT_ID,
    'Api-Key': API_KEY,
    'Content-Type': 'application/json'
}


def fin_trans_for_period(date_start, date_end):
    body = {
        "filter": {
            "date": {
                "from": date_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to": date_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            },
            "operation_type": [],
            "posting_number": "",
            "transaction_type": "all"
        },
        "page": 1,
        "page_size": 1000
    }
    response = requests.post('https://api-seller.ozon.ru/v3/finance/transaction/list', json=body, headers=headers)
    jason = response.json()
    df_final = pd.DataFrame(jason['result']['operations'])
    size = df_final.shape[0]
    i = 2
    while size == 1000:
        body = {
            "filter": {
                "date": {
                    "from": date_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "to": date_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                },
                "operation_type": [],
                "posting_number": "",
                "transaction_type": "all"
            },
            "page": i,
            "page_size": 1000
        }
        response = requests.post('https://api-seller.ozon.ru/v3/finance/transaction/list', json=body, headers=headers)
        jason = response.json()
        df = pd.DataFrame(jason['result']['operations'])
        size = df.shape[0]
        df_final = pd.concat([df_final, df], axis=0)
        i += 1
    return df_final


def get_offer_sku_table(offer_ids):
    body = {
        "offer_id": offer_ids
    }
    response = requests.post(API_URL + "/v3/product/info/list", json=body, headers=headers)
    jason = response.json()
    df = pd.json_normalize(jason['result']['items'])
    return df[['offer_id', 'sku', 'marketing_price']]


def product_list():
    body = {
        "filter": {
            "visibility": "ALL"
        },
        "last_id": "",
        "limit": 1000
    }
    response = requests.post("https://api-seller.ozon.ru/v3/product/list", json=body, headers=headers)
    jason = response.json()
    return pd.DataFrame(jason['result']['items'])


def get_sku(x):
    return x[0]['sku']


def turnover(day):
    df = fin_trans_for_period(day - dt.timedelta(30), day)

    df = df[df['operation_type'] == 'OperationAgentDeliveredToCustomer']
    df['sku'] = df['items'].apply(get_sku)
    offer_id = product_list()['offer_id'].values.tolist()
    df = df.merge(get_offer_sku_table(offer_id), how='left', left_on='sku', right_on='sku')
    df = df.groupby('offer_id', as_index=False).count()[['offer_id', 'operation_id']]
    moq = pd.read_csv('moq.csv')
    moq.columns = ['offer_id', 'moq']
    df = df.merge(moq, how='left', left_on='offer_id', right_on='offer_id')
    df['turnover'] = df.apply(lambda x: 35 + x['moq'] * 30 / x['operation_id'], axis=1)
    return df[['offer_id', 'turnover']]


def turnover_for_month(day):
    df = fin_trans_for_period(day - dt.timedelta(30), day)
    df2 = fin_trans_for_period(day - dt.timedelta(60), day - dt.timedelta(31))
    df = pd.concat([df, df2], axis=0)
    df = df[df['operation_type'] == 'OperationAgentDeliveredToCustomer']
    df['date'] = pd.to_datetime(df['operation_date'])
    df['sku'] = df['items'].apply(get_sku)
    offer_id = product_list()['offer_id'].values.tolist()
    df = df.merge(get_offer_sku_table(offer_id), how='left', left_on='sku', right_on='sku')
    moq = pd.read_csv('moq.csv')
    moq.columns = ['offer_id', 'moq']
    ans = []
    for i in range(30):
        tmp = df[(df['date'] <= (day - dt.timedelta(i))) & (df['date'] >= day - dt.timedelta(30 + i))]
        tmp = tmp.groupby('offer_id', as_index=False).count()[['offer_id', 'operation_id']]
        tmp = tmp.merge(moq, how='left', left_on='offer_id', right_on='offer_id')
        tmp['turnover'] = tmp.apply(lambda x: 35 + x['moq'] * 30 / x['operation_id'], axis=1)
        ans.append([tmp[['offer_id', 'turnover']], day - dt.timedelta(i)])
    return ans
