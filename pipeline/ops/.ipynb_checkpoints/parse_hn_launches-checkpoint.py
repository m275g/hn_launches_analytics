import os, sys, time, random
from typing import Tuple
import requests as r 
import json
from bs4 import BeautifulSoup
import datetime as dt
import numpy as np
import pandas as pd
import clickhouse_driver
from clickhouse_driver.client import Client
from utils.utils import *
from dagster import op, Out, In

import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)


@op(description = 'Parse launches from HN')
def parse_hn_launches(context) -> dict:
    
    
    #[Count HN pages to parse]
    clickhouse = Client('85.193.83.20', database = 'hn_launches',
                        user = 'admin', password = '0987654321')
    
    ch_items = clickhouse.query_dataframe('select uniq(item_id) as items from hn_launches.launches;')
    
    if ch_items['items'][0] == 0: pages = 11
    else: pages = 1
    
    context.log.info(f'HN pages to parse: {pages}')
    
    
    #[Parsing HN launches_table]
    url = 'https://news.ycombinator.com/launches'

    launches_table = pd.DataFrame(columns = ['item_id', 'title', 'url'])

    for i in range(pages):

        time.sleep(np.random.poisson(5))

        s = create_session()

        response = s.get(url if i == 0 else url+'?next='+str(next_item_id))
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', class_ = 'itemlist')
            launches = table.find_all('tr', class_ = 'athing')

            next_item_id = table.find('a', class_ = 'morelink').attrs['href'].replace('launches?next=', '')

            for launch in launches:
                launches_table = launches_table.append(
                    {'item_id': launch['id'], 
                     'title': launch.find('a', class_ = 'titlelink').text,
                     'url': launch.find('a', class_ = 'titlelink')['href']},
                    ignore_index = True)

            launches_table.append(launches_table)

        else: 
            raise Exception(f'HN/launches_table, Error: {response.status_code}')
            #break

        if next_item_id == None: break

    launches_table = launches_table.astype({'item_id': 'int'})
    
    context.log.info(f'Parsed NH/launches_table')
    
    
    #[Parsing HN launches pages & comments]
    item_url = 'https://news.ycombinator.com/item?id='

    launches_pages = pd.DataFrame(columns = ['item_id', 'score', 'by', 'time', 'text'])
    launches_comments = pd.DataFrame(columns = ['item_id', 'comment_id', 'by', 'time', 'level', 'comment'])

    #attempts = 0
    #while attempts < 10:

    s = create_session()
    for count, item_id in enumerate(launches_table['item_id']):

        time.sleep(np.random.poisson(10))

        if count % 10 == 0: 
            s = create_session()

        #if count % 90 == 0: #select new ip

        #s.headers.update({'referer': url})
        response = s.get(item_url+str(item_id))

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            launch_page = soup.find('table', class_ = 'fatitem')

            launches_pages = launches_pages.append(
                {'item_id': item_id,
                 'score': launch_page.find('span', class_ = 'score').text.replace('points', ''),
                 'by': launch_page.find('a', class_ = 'hnuser').text,
                 'time': launch_page.find('span', class_ = 'age')['title'],
                 'text': max([text.text for text in launch_page.find_all('td')], key = len)},
                ignore_index = True)

            launches_pages.append(launches_pages)

            comments_table = soup.find('table', class_ = 'comment-tree')

            comments = soup.find_all('tr', class_ = 'athing comtr')

            for comment in comments:
                launches_comments = launches_comments.append(
                    {'item_id': item_id,
                     'comment_id': comment.attrs['id'],
                     'by': '' if any(d in comment.find('div', class_ = 'comment').text for d in ['[deleted]', '[dead]']) \
                              else comment.find('a', class_ = 'hnuser').text,
                     'time': comment.find('span', class_ = 'age').attrs['title'],
                     'level': comment.find('td', class_ = 'ind').attrs['indent'],
                     'comment': comment.find('div', class_ = 'comment').text.replace('\n', '')},
                    ignore_index = True)

            launches_comments.append(launches_comments)

        else:
            raise Exception(f'NH/launches_pages, Error: {response.status_code}')
            #attempts += 1
            #if response.status_code == 403: 
                #mark ip as banned
                #select new ip
            #break

    launches_pages = launches_pages.astype({'item_id': 'int', 'score': 'int'})
    launches_pages['time'] = pd.to_datetime(launches_pages['time']) 
    
    context.log.info(f"Parsed {len(launches_pages['item_id'])} HN launches")

    launches_comments = launches_comments.astype({'item_id': 'int', 'comment_id': 'int', 'level': 'int'})
    launches_comments['time'] = pd.to_datetime(launches_comments['time'])
    
    context.log.info(f"Parsed {len(launches_comments['item_id'])} HN comments")
    
    return {'launches_table': launches_table, 
            'launches_pages': launches_pages, 
            'launches_comments': launches_comments}
