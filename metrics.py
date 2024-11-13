import datetime

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
def articl_list(data):
    headers = {'Content-type': 'application/json', 'Client-Id': CLIENT_ID, 'Api-Key': API_KEY}
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
def create_orders_report(data):
    headers = {'Content-type': 'application/json', 'Client-Id': CLIENT_ID, 'Api-Key': API_KEY}
    url = 'https://api-seller.ozon.ru/v1/report/postings/create'
    data = data
    response = r.post(url, data=json.dumps(data), headers=headers, verify=True)
    if response.status_code == 200:
        answer = eval(response.content.decode('utf-8'))
        return response.status_code, answer.get('result').get('code')
    else:
        return response.status_code, "something_wrong", response


# получение ссылки на отчет
def get_report_link(report):
    headers = {'Content-type': 'application/json', 'Client-Id': CLIENT_ID, 'Api-Key': API_KEY}
    url = 'https://api-seller.ozon.ru/v1/report/info'
    data = {
        "code": report
    }
    while True:
        response = r.post(url, data=json.dumps(data), headers=headers, verify=True)
        if response.status_code == 200:
            answer = eval(response.content.decode('utf-8'))
            if answer.get('result').get('status') == 'success':
                return response.status_code, answer.get('result').get('file')
            if answer.get('result').get('status') == 'processing' or answer.get('result').get('status') == 'waiting':
                sleep(2)
                continue
            else:
                return response.status_code, "something_wrong"
        else:
            return response.status_code, "something_wrong"


def get_offer_id_list():
    x = articl_list(data={
        "filter": {},
        "last_id": "",
        "limit": 1000
    })
    return [i['offer_id'] for i in x[1]]


def get_date(date):
    return date[:10]


def print_graph_for_offer_id(offer_ids):
    date_to = datetime.date.today()
    delta = datetime.timedelta(30)
    date_from = date_to - delta
    traces = []
    for offer_id in offer_ids:
        ass = create_orders_report(data={
            "filter": {
                "processed_at_from": date_from.strftime('%Y-%m-%d') + "T00:00:00.861Z",
                "processed_at_to": date_to.strftime('%Y-%m-%d') + "T00:00:00.861Z",
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
        link = get_report_link(ass[1])
        report = pd.read_csv(link[1], sep=';')
        orders = report[['Article code', 'Quantity', 'Accepted for processing', 'Status']]
        orders = orders.rename(
            columns={'Article code': 'article', 'Quantity': 'quantity', 'Accepted for processing': 'date',
                     'Status': 'status'})
        orders['date'] = orders['date'].apply(get_date)
        fig = px.bar(orders, x="date", y="quantity", color="status", title=offer_id, barmode='relative')
        st.plotly_chart(fig)
