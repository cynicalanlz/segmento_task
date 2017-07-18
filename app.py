from pypodio2 import api
from client_secret import *
import pprint
import json
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from aiohttp import web
import aiohttp_jinja2
import jinja2

def get_data(c):
    # Org - Компания

    orgs = [item['org_id'] for item in c.Org.get_all()]

    spaces = [space for space in c.Space.find_all_for_org(orgs[0])]
    spaces = dict([(space['name'], space['space_id']) for space in spaces])

    apps = []

    for key in spaces.keys():
        for app in c.Application.list_in_space(spaces[key]):
            apps.append((app['config']['item_name'], app['app_id']))


    apps = dict(apps)


    mediaplans = c.Item.filter(apps['Mediaplan'],{'limit' : 500})
    mediaplans_items = []

    for item in mediaplans['items']:
        mediaplan = {}
        for field in item['fields']:        
            vals = field['values']
            label = field['label']

            if label == 'Status':
                vals = vals[0]['value']['text']
            elif label == 'Account Manager':
                vals = vals[0]['value'].strip('</p>').strip('<p>')
            elif label == 'Mediaplan Name':
                vals = vals[0]['value']
            elif label == 'Date':
                vals = vals[0]['start_date']

            mediaplan[label] = vals

        if mediaplan != {}:
            mediaplans_items.append(mediaplan)

    mediaplans_df = pd.DataFrame.from_dict(mediaplans_items)
    mediaplans_df['Date'] = pd.to_datetime(mediaplans_df['Date'])
    start_date = datetime.now() - timedelta(days=90)
    mediaplans_df = mediaplans_df[mediaplans_df['Date'] > start_date]
    mediaplans_df = mediaplans_df[mediaplans_df['Status'] == 'Confirmed']

    print(mediaplans_df)

    account_manager_items = []
    account_manager_df = mediaplans_df.groupby(['Account Manager'])['Mediaplan Name'].nunique()


    campaigns = c.Item.filter(apps['Campaign'],{'limit' : 500})
    campaigns_items = []

    for item in campaigns['items']:
        campaign = {}

        for field in item['fields']:
            vals = field['values']
            label = field['label']      

            if label == 'Status':
                vals = vals[0]['value']['text']
                if vals!='Active':
                    break

            elif label == 'Category':
                vals = vals[0]['value']['text']
                if vals!='Desktop':
                    break
            elif label == 'Campaign Name':
                vals = vals[0]['value']
            elif label == 'Mediaplan':
                vals = vals[0]['value']['title']
            else:
                continue            

            campaign[label] = vals

        campaigns_items.append(campaign)



    campaigns_df = pd.DataFrame.from_dict(campaigns_items)

    df3 = mediaplans_df.merge(
        campaigns_df, 
        left_on='Mediaplan Name', right_on='Mediaplan', 
        how='outer'
        )
    print(df3)

    has_campaign = df3['Campaign Name'].notnull()
    is_active = df3['Status_y'] == 'Active'

    converted = df3[ has_campaign & is_active ]['Mediaplan Name'].nunique() / df3['Mediaplan Name'].nunique()

    print(converted)

    has_campaign_num = df3[has_campaign]['Mediaplan Name'].nunique()

    print(has_campaign_num)

    return account_manager_df, converted, has_campaign_num



class Handler:
    def __init__(self):
        pass

    @asyncio.coroutine
    def handle_data(self, request):

        c = api.OAuthClient(
            client_id,
            client_secret,
            username,
            password
        )

        account_manager_df, converted, has_campaign_num = get_data(c)

        context = {
            'table' : account_manager_df.to_frame().to_html(),
            'converted' : converted,
            'has_campaign_num' : has_campaign_num
        }

        response = aiohttp_jinja2.render_template(
            "home.html",
            request,
            context
            )
           
        response.headers['Content-Language'] = 'en'
        return response


def main():
    # starting thread spawning application
    # тут запускается веб-приложение на aiohttp, которое обрабатывает запросы
    # на открытие slack_messaging
    app = web.Application()
    handler = Handler()
    app.router.add_get('/', handler.handle_data)
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('/home/usr/dev/segmento/templates'))

    web.run_app(app, host='127.0.0.1', port=8081)

if __name__ == "__main__":
    main()

