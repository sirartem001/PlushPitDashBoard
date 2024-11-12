import streamlit as st
import pandas as pd
import requests as r
import json
import plotly.express as px
from plotly.subplots import make_subplots
from time import sleep


API_KEY = '22c5ec5b-5e3f-4002-afca-95fbbdae08aa'
CLIENT_ID = '1997569'


# список артикулов
def articl_list(client_id, api_key, data):
    headers = {'Content-type': 'application/json', 'Client-Id': client_id, 'Api-Key': api_key}
    url = 'https://api-seller.ozon.ru/v2/product/list'
    data = data
    response = r.post(url, data=json.dumps(data), headers=headers)
    if response.status_code == 200:
        answer = response.content.decode('utf-8')
        answer = answer.replace('true', """ 'da' """)
        answer = answer.replace('false', """ 'net' """)
        answer = eval(answer)
        return response.status_code, answer.get('result').get('items')
    else:
        return response.status_code, "something_wrong"


# отчет о заказах
def create_orders_report(client_id, key, data):
    headers = {'Content-type': 'application/json', 'Client-Id': client_id, 'Api-Key': key}
    url = 'https://api-seller.ozon.ru/v1/report/postings/create'
    data = data
    response = r.post(url, data=json.dumps(data), headers=headers, verify=True)
    if response.status_code == 200:
        answer = eval(response.content.decode('utf-8'))
        return response.status_code, answer.get('result').get('code')
    else:
        return response.status_code, "something_wrong", response


# получение ссылки на отчет
def get_report_link(client_id, key, report):
    headers = {'Content-type': 'application/json', 'Client-Id': client_id, 'Api-Key': key}
    url = 'https://api-seller.ozon.ru/v1/report/info'
    data = {
        "code": report
    }

    response = r.post(url, data=json.dumps(data), headers=headers, verify=True)
    if response.status_code == 200:
        answer = eval(response.content.decode('utf-8'))
        if answer.get('result').get('status') == 'success':
            return response.status_code, answer.get('result').get('file')
        else:
            return response.status_code, answer.get('result').get('status')
    else:
        return response.status_code, "something_wrong"


def get_offer_id_list():
    x = articl_list(client_id=CLIENT_ID, api_key=API_KEY, data={
        "filter": {},
        "last_id": "",
        "limit": 1000
    })
    return [i['offer_id'] for i in x[1]]


def print_graph_for_offer_id(offer_ids, date_from, date_to):
    fig = make_subplots(rows=len(offer_ids), cols=1, shared_yaxes=True)
    traces = []
    for offer_id in offer_ids:
        ass = create_orders_report(CLIENT_ID, API_KEY, data={
            "filter": {
                "processed_at_from": "2024-10-28T03:00:00.861Z",
                "processed_at_to": "2024-11-12T23:59:59.861Z",  # перевод в UTC+3
                "delivery_schema": [
                    "fbo"
                ],
                "sku": [],
                "cancel_reason_id": [],
                "offer_id": offer_id,
                "status_alias": [],
                "statuses": [],
                "title": ""
            },
            "language": "EN"
        })
        sleep(1)
        link = get_report_link(CLIENT_ID, API_KEY, ass[1])
        report = pd.read_csv(link[1], sep=';')
        orders = report[['Article code', 'Quantity', 'Accepted for processing', 'Status']]
        orders = orders.rename(
            columns={'Article code': 'article', 'Quantity': 'quantity', 'Accepted for processing': 'date',
                     'Status': 'status'})
        for i in range(orders.index.min(), orders.index.max() + 1):
            orders.date[i] = orders.date[i][:10]
        fig = px.bar(orders, x="date", y="quantity", color="status", title=offer_id)
        traces.append([])
        for trace in range(len(fig['data'])):
            traces[-1].append(fig["data"][trace])
    this_figure = make_subplots(rows=len(offer_ids), cols=1)
    for i in range(len(traces)):
        for trace in traces[i]:
            this_figure.add_trace(trace, row=i + 1, col=1)
    st.plotly_chart(this_figure)


offer_ids = ['tochilka_bear_pink_dark', 'tochilka_cat_yellow']
print_graph_for_offer_id(offer_ids, 0, 0)
