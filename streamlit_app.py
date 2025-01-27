import altair as alt
import pandas as pd
import streamlit as st
import json
import io
import datetime
import requests
from metrics import *
import paramiko
from Turnover import turnover, turnover_for_month
import plotly.express as px
import plotly.graph_objs as go

API_URL = 'https://api-seller.ozon.ru'
API_KEY = '22c5ec5b-5e3f-4002-afca-95fbbdae08aa'
CLIENT_ID = '1997569'
ERROR = ""
now = datetime.datetime.now()
headers = {
    'Client-Id': CLIENT_ID,
    'Api-Key': API_KEY,
    'Content-Type': 'application/json'
}

st.set_page_config(layout="wide", page_title="PlushPit DashBoard")
st.title("PlushPit DashBoard")


def get_ozon_prod_state(status):
    now = datetime.datetime.now()
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
        "cursor": last_id
    }
    response = requests.post(API_URL + "/v4/product/info/stocks", json=body, headers=headers)
    jason = response.json()
    with open('tmp.json', 'w') as json_file:
        json.dump(jason, json_file, indent=4)
    df = pd.read_json('tmp.json')
    if df.empty:
        return pd.DataFrame(), ''
    last = df.iloc[0]['cursor']
    df = df['items'].apply(pd.Series)

    df = pd.concat([df.drop(columns='stocks'), df['stocks'].apply(lambda x: next((y for y in x if y['type'] == 'fbo'), None))], axis=1)
    df.dropna(inplace=True, how='any')
    df = df.drop(columns=['product_id'])
    df = pd.concat([df, df['stocks'].apply(pd.Series)], axis=1)
    df = df.drop(columns=['stocks', 'type'])
    df = resolve_x2(df[['offer_id', 'present', 'reserved']], ['present', 'reserved'])
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
                    df.loc[len(df.index), column_name] = quantity
                else:
                    df.at[it[0], column_name] = quantity * 2 + df.at[it[0], column_name]
            x2_names.append(string)
    return df.loc[~df['offer_id'].isin(x2_names)]


def get_in_way():
    in_way = get_ozon_prod_state("delivering")
    # in_way = pd.concat([in_way, get_ozon_prod_state("awaiting_deliver")])
    # in_way = pd.concat([in_way, get_ozon_prod_state("awaiting_packaging")])
    # in_way = pd.concat([in_way, get_ozon_prod_state("cancelled")])
    in_way = in_way.groupby('offer_id')['quantity'].sum().reset_index()
    in_way = resolve_x2(in_way, ['quantity'])
    in_way.columns = ['offer_id', 'in_way']
    return in_way.fillna(0)


def get_pivot():
    response = requests.get(
        'https://docs.google.com/spreadsheets/d/12pb0_QUhnZDQ6eDb87Wlj7w0L5r9VV0dccAvzpx41KU/gviz/tq?tqx=out:csv&gid=1823642991')
    assert response.status_code == 200, 'Wrong status code'
    df = pd.read_csv(io.StringIO(response.text))
    df = df[['Сводная по остаткам', 'Unnamed: 2']]
    df.drop(index=df.index[0], axis=0, inplace=True)
    df = df[df['Сводная по остаткам'].notna()]
    df.columns = ['offer_id', 'pivot']
    df = df.fillna(0)
    return df


def get_transfer_data(order_id):
    body = {
        "posting_number": order_id
    }
    response = requests.post(API_URL + "/v2/posting/fbo/get", json=body, headers=headers)
    jason = response.json()
    if 'items' not in jason:
        ERROR = str(jason['message'])
        return pd.DataFrame({'offer_id': [], 'quantity': []})
    with open('tmp.json', 'w') as json_file:
        json.dump(jason['items'], json_file, indent=4)
    df = pd.read_json('tmp.json')
    return df.drop(columns=['icon_path', 'sku', 'name'])


def get_ozon_transfer():
    body = {
        "dir": "DESC",
        "filter": {
            "states": [
                "ORDER_STATE_IN_TRANSIT",
                "ORDER_STATE_DATA_FILLING",
                "ORDER_STATE_READY_TO_SUPPLY",
                "ORDER_STATE_ACCEPTED_AT_SUPPLY_WAREHOUSE"
            ]
        },
        "paging": {
            "from_supply_order_id": 0,
            "limit": 100
        }
    }
    response = requests.post(API_URL + "/v2/supply-order/list", json=body, headers=headers)
    jason = response.json()
    if 'supply_order_id' not in jason:
        ERROR = str(jason['message'])
        return pd.DataFrame({'offer_id': [], 'quantity': []})
    with open('tmp.json', 'w') as json_file:
        json.dump(jason['supply_order_id'], json_file, indent=4)
    id = pd.read_json('tmp.json').values.tolist()
    order_id = []
    for i in id:
        order_id += i
    df = pd.DataFrame({'offer_id': [], 'quantity': []})
    for i in order_id:
        df = pd.concat([df, get_transfer_data(i)])
    df = df.groupby('offer_id')['quantity'].sum().reset_index()
    df = resolve_x2(df, ['quantity'])
    df.columns = ['offer_id', 'transfer']
    return df


def foo(x, y, z, w):
    return x - y - z - w

# Подсчет остатков на складе
@st.cache_data
def load_dataWareHouse():
    TIME = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2) + ' ' + str(now.hour + 3).zfill(
        2) + ':' + str(now.minute).zfill(2) + ':' + str(now.second).zfill(2)
    pivot = get_pivot()
    in_way = get_in_way()
    trans = get_ozon_transfer()
    fbo = get_fbo()
    result = pd.merge(pivot, in_way, on="offer_id", how="outer")
    result = pd.merge(result, fbo, on="offer_id", how="outer")
    result = pd.merge(result, trans, on="offer_id", how="outer")
    result = result.fillna(0)
    ERROR = ""
    result['warehouse'] = result.apply(lambda x: foo(x.pivot, x.reserved, x.in_way, x.present), axis=1)
    return result.reset_index(), TIME


def refreshWareHouse():
    load_dataWareHouse.clear()
    dfWareHouse, TIMEWareHouse = load_dataWareHouse()


def resolve_actions(actions):
    titles = ''
    if len(actions) == 0:
        return ''
    for action in actions:
        titles += action['title'] + '; '
    return titles


def get_all_coast():
    body = {
        "filter": {
            "visibility": "ALL"
        },
        "last_id": "",
        "limit": 1000
    }
    response = requests.post(API_URL + "/v4/product/info/prices", json=body, headers=headers)
    jason = response.json()
    assert 'result' in jason, str(jason['message'])
    return jason['result']


@st.cache_data
def load_dataPrice():
    TIME = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2) + ' ' + str(now.hour + 3).zfill(
        2) + ':' + str(now.minute).zfill(2) + ':' + str(now.second).zfill(2)
    df1 = pd.json_normalize(get_all_coast()['items'])

    df1 = df1[['offer_id', 'price.price', 'price.marketing_price', 'commissions.sales_percent',
               'commissions.fbo_deliv_to_customer_amount', 'marketing_actions.actions']]

    df1.columns = ['offer_id', ',базовая цена', 'цена на карточке', 'комиссия за продажу',
                   'комиссия за доставку покупателю',
                   'акции']
    df1['акции'] = df1['акции'].apply(lambda d: d if isinstance(d, list) else [])
    df1['акции'] = df1['акции'].apply(resolve_actions)
    df1['комиссия за продажу'] = pd.to_numeric(
        pd.to_numeric(df1['комиссия за продажу']) * pd.to_numeric(df1['цена на карточке'])).div(100.)
    return df1, TIME


def refreshPrice():
    load_dataPrice.clear()
    Price, TIMEPrice = load_dataWareHouse()


tab_titles = ['Остатки на складе', 'Цены', 'Графики метрик', 'Параметры автоцен', 'Оборачиваемость']
tab1, tab2, tab3, tab4, tab5 = st.tabs(tab_titles)
with tab1:
    dfWareHouse, TIMEWareHouse = load_dataWareHouse()

    st.text("Последнее обновление: " + TIMEWareHouse)
    st.button("↻ refresh", on_click=refreshWareHouse, key=0)
    st.dataframe(
        dfWareHouse,
        use_container_width=True
    )
    st.text(ERROR)
    ERROR = ""
    st.text("pivot - общий остаток")
    st.text("in_way - едут от склада Озон до клиента")
    st.text("present - лежат на складе Озон")
    st.text("reserved - зарезервированны на складе Озон")
    st.text("warehouse - лежат на нашем складе")
with tab2:
    Price, TIMEPrice = load_dataPrice()
    st.text("Последнее обновление: " + TIMEPrice)
    st.button("↻ refresh", on_click=refreshPrice, key=1)
    st.table(
        Price.sort_values("offer_id")
    )
options = []
options_turnover = []
articl = []


def plot():
    article = []
    for i in range(len(options)):
        if options[i]:
            article.append(offer_ids[i])
    with tab3:
        print_graph_for_offer_id(article)


offer_ids_to = []


def plot_turnover(turn_over_month):
    article = []
    for i in range(len(options_turnover)):
        if options_turnover[i]:
            article.append(offer_ids_to[i])
    for i in range(len(turn_over_month)):
        turn_over_month[i][0] = turn_over_month[i][0].merge(pd.Series(article, name='offer_id'), how='right',
                                                            left_on='offer_id', right_on='offer_id')
    fig = go.Figure()
    for offer in article:
        days = []
        tover = []
        for date in turn_over_month:
            tover.append(date[0][date[0]['offer_id'] == offer].values[0, 1])
            days.append(date[1])
        fig.add_trace(go.Scatter(x=days, y=tover, name=offer))
    with tab5:
        st.plotly_chart(fig)


with tab3:
    st.write('Выбираем нужные артикулы:')
    columns = st.columns(5)
    offer_ids = get_offer_id_list()
    offer_ids.sort()
    col = 0
    for offer_id in offer_ids:
        options.append(columns[col % 5].checkbox(offer_id))
        col += 1
    st.button("Отрисовать графики", on_click=plot, key=2)

with tab4:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    source_ip = "147.45.254.148"
    source_username = "root"
    source_password = "u-W??cw#Vr7iMS"
    ssh.connect(source_ip, username=source_username, password=source_password)
    sftp = ssh.open_sftp()
    file_to_transfer = '/root/PlushPit/PlushPitFinance/price_list.csv'
    destination_path = 'price_list.csv'

    sftp.get(file_to_transfer, destination_path)
    sftp.close()
    ssh.close()
    df = pd.read_csv("price_list.csv").drop(columns='Unnamed: 0')
    subset = df[['цена норм', 'цена от Артема']]
    df['цена базовая'] = subset.max(axis=1)
    st.dataframe(df[['Артикул', 'Группа', 'цена базовая', 'цена мин']], use_container_width=True)

with tab5:
    now = datetime.datetime.now()
    turn = turnover(now)
    st.dataframe(turn, use_container_width=True)
    to = turnover_for_month(now)
    st.write('Выбираем нужные артикулы:')
    columns_turnover = st.columns(5)
    offer_ids_to = turn['offer_id'].tolist()
    offer_ids_to.sort()
    col = 0
    for offer_id in offer_ids_to:
        options_turnover.append(columns_turnover[col % 5].checkbox(offer_id, key=1000 + col))
        col += 1
    st.button("Отрисовать график", on_click=plot_turnover, key=123, kwargs={"turn_over_month": to})
