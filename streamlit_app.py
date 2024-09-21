import altair as alt
import pandas as pd
import streamlit as st
import json
import io
from datetime import datetime
import requests

API_URL = 'https://api-seller.ozon.ru'
API_KEY = '22c5ec5b-5e3f-4002-afca-95fbbdae08aa'
CLIENT_ID = '1997569'
ERROR = ""
now = datetime.now()
TIME = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2) + 'T' + str(now.hour).zfill(
        2) + ':' + str(now.minute).zfill(2) + ':' + str(now.second).zfill(2) + '.000Z'
headers = {
    'Client-Id': CLIENT_ID,
    'Api-Key': API_KEY,
    'Content-Type': 'application/json'
}

st.set_page_config(page_title="Количество товаров на складах")
st.title("Количество товаров на складах")





def get_ozon_prod_state(status):
    now = datetime.now()
    date1 = str(now.year - 1) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2) + 'T' + str(now.hour).zfill(
        2) + ':' + str(now.minute).zfill(2) + ':' + str(now.second).zfill(2) + '.000Z'
    date2 = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2) + 'T' + str(now.hour).zfill(
        2) + ':' + str(now.minute).zfill(2) + ':' + str(now.second).zfill(2) + '.000Z'
    body = {
        "dir": "DESC",
        "filter": {
            "since": date1,
            "status": status,
            "to": date2
        },
        "limit": 1000,
        "offset": 0,
        "translit": False,
        "with": {
            "analytics_data": False,
            "financial_data": False
        }
    }
    response = requests.post(API_URL + "/v2/posting/fbo/list", json=body, headers=headers)
    jason = response.json()
    if 'result' not in jason:
        ERROR = str(jason['message'])
        return pd.DataFrame({'offer_id': [], 'quantity': []})
    with open('tmp.json', 'w') as json_file:
        json.dump(jason['result'], json_file, indent=4)
    df = pd.read_json('tmp.json')
    df = df['products'].apply(pd.Series)
    df = pd.DataFrame(df[0].values.tolist())
    return df

def get_ozon_warehouse_state(last_id):
    body = {
        "dir": "DESC",
        "filter": {
            "visibility": "ALL"
        },
        "limit": 1000,
        "last_id": last_id
    }
    response = requests.post(API_URL + "/v3/product/info/stocks", json=body, headers=headers)
    jason = response.json()
    if 'result' not in jason:
        ERROR = str(jason['message'])
        return pd.DataFrame({'offer_id': [], 'present': [], 'reserved': []})
    with open('tmp.json', 'w') as json_file:
        json.dump(jason['result'], json_file, indent=4)
    df = pd.read_json('tmp.json')
    if df.empty:
      return pd.DataFrame(), ''
    last = df.iloc[0]['last_id']
    df = df['items'].apply(pd.Series)
    df = pd.concat([df, df['stocks'].apply(pd.Series)], axis=1)
    df.columns = ['0', 'offer_id', '1', '2', '3', '4']
    df = df.drop(columns=['1', '0', '2', '4'])
    df = pd.concat([df, df['3'].apply(pd.Series)], axis=1)
    df = df.drop(columns=['3', 'type'])
    df = resolve_x2(df, ['present', 'reserved'])
    return df, last

def get_fbo():
    df, last_id = get_ozon_warehouse_state("")
    while True:
      newdf, last_id = get_ozon_warehouse_state(last_id)
      if newdf.empty:
        return df
      df = pd.concat(df, newdf)


def resolve_x2(df, column_names):
    x2_names = []
    for ind in df.index:
        string = df['offer_id'][ind]
        if string.startswith('x2_'):
            it = df.index[df['offer_id'] == string[3:]].tolist()
            for column_name in column_names:
              quantity = df.loc[df['offer_id'] == string][column_name].tolist()[0]
              if len(it) == 0:
                  df.loc[len(df.index)] = [string[3:], quantity]
              else:
                  df.at[it[0], column_name] = quantity * 2 + df.at[it[0], column_name]
            x2_names.append(string)
    return df.loc[~df['offer_id'].isin(x2_names)]


def get_in_way():
    in_way = get_ozon_prod_state("delivering")
    in_way = pd.concat([in_way, get_ozon_prod_state("awaiting_deliver")])
    #in_way = pd.concat([in_way, get_ozon_prod_state("awaiting_packaging")])
    #in_way = pd.concat([in_way, get_ozon_prod_state("cancelled")])
    in_way = in_way.groupby('offer_id')['quantity'].sum().reset_index()
    in_way = resolve_x2(in_way, ['quantity'])
    in_way.columns = ['offer_id', 'in_way']
    return in_way.fillna(0)

def get_pivot():
    response = requests.get(
        'https://docs.google.com/spreadsheets/d/12pb0_QUhnZDQ6eDb87Wlj7w0L5r9VV0dccAvzpx41KU/gviz/tq?tqx=out:csv&sheet={Pivot}')
    assert response.status_code == 200, 'Wrong status code'
    df = pd.read_csv(io.StringIO(response.text))
    df = df[['Сводная по остаткам', 'Unnamed: 2']]
    df.drop(index=df.index[0], axis=0, inplace=True)
    df = df[df['Сводная по остаткам'].notna()]
    df.columns = ['offer_id', 'pivot']
    df = df.fillna(0)
    return df

def foo(x, y, z, w):
    return x - y - z - w

@st.cache_data
def load_data():
    pivot = get_pivot()
    in_way = get_in_way()
    fbo = get_fbo()
    result = pd.merge(pivot, in_way, on="offer_id", how="outer")
    result = pd.merge(result, fbo, on="offer_id", how="outer")
    result = result.fillna(0)
    result['warehouse'] = result.apply(lambda x: foo(x.pivot, x.reserved, x.in_way, x.present), axis=1)
    return result

df = load_data()

def refresh():
    load_data.clear()
    df = load_data()
    TIME = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2) + 'T' + str(now.hour).zfill(
        2) + ':' + str(now.minute).zfill(2) + ':' + str(now.second).zfill(2) + '.000Z'

st.text("Последнее обновление: " +TIME)
st.button("↻ refresh", on_click=refresh)
st.dataframe(
    df.sort_values("offer_id"),
    use_container_width=True
)
st.text(ERROR)
ERROR = ""