import altair as alt
import pandas as pd
import streamlit as st
import json
import io
from datetime import datetime
import streamlit as st
import requests

API_URL = 'https://api-seller.ozon.ru'
API_KEY = '22c5ec5b-5e3f-4002-afca-95fbbdae08aa'
CLIENT_ID = '1997569'

headers = {
    'Client-Id': CLIENT_ID,
    'Api-Key': API_KEY,
    'Content-Type': 'application/json'
}

st.set_page_config(page_title="Количество товаров на складах", page_icon="🎬")
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
        print(jason['message'])
        return pd.DataFrame({'offer_id': [], 'quantity': []})
    with open('tmp.json', 'w') as json_file:
        json.dump(jason['result'], json_file, indent=4)
    df = pd.read_json('tmp.json')
    df = df['products'].apply(pd.Series)
    df = pd.DataFrame(df[0].values.tolist())
    return df


def resolve_x2(df):
    x2_names = []
    for ind in df.index:
        string = df['offer_id'][ind]
        if string.startswith('x2_'):
            it = df.index[df['offer_id'] == string[3:]].tolist()
            quantity = df.loc[df['offer_id'] == string]['quantity'].tolist()[0]
            if len(it) == 0:
                df.loc[len(df.index)] = [string[3:], quantity]
            else:
                df.at[it[0], 'quantity'] = quantity * 2 + df.at[it[0], 'quantity']
            x2_names.append(string)
    return df.loc[~df['offer_id'].isin(x2_names)]


def get_ozon_warehouse_state():
    in_way = get_ozon_prod_state("delivering")
    in_way = pd.concat([in_way, get_ozon_prod_state("cancelled")])
    in_way = in_way.groupby('offer_id')['quantity'].sum().reset_index()
    in_way = resolve_x2(in_way)

    fbo = get_ozon_prod_state("awaiting_packaging")
    fbo = pd.concat([fbo, get_ozon_prod_state("awaiting_deliver")])
    fbo = fbo.groupby('offer_id')['quantity'].sum().reset_index()
    fbo = resolve_x2(fbo)
    fbo.columns = ['offer_id', 'fbo']
    in_way.columns = ['offer_id', 'in_way']
    result = pd.merge(fbo, in_way, on="offer_id", how="outer")
    return result.fillna(0)


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


def foo(x, y, z):
    return x - y - z

@st.cache_data
def load_data():
    pivot = get_pivot()
    ozon = get_ozon_warehouse_state()
    result = pd.merge(pivot, ozon, on="offer_id", how="outer")
    result = result.fillna(0)
    result['warehouse'] = result.apply(lambda x: foo(x.pivot, x.fbo, x.in_way), axis=1)
    return result

df = load_data()

st.dataframe(
    df,
    use_container_width=True
)