import os, sys, time, random
from typing import Tuple
import requests as r
import json
import yaml
from yaml.loader import SafeLoader
from bs4 import BeautifulSoup
import datetime as dt
import numpy as np
import pandas as pd
import clickhouse_driver
from clickhouse_driver.client import Client
from utils.utils import *
from dagster import op, Out, In


with open('/opt/dagster/app/ops/config.yaml') as c:
    config = yaml.load(c, Loader = SafeLoader)


@op(description = 'Parse repos statistics from Github')
def parse_github(context) -> pd.core.frame.DataFrame:


    clickhouse = Client(host = config['clickhouse']['host'], database = 'hn_launches',
                        user = config['clickhouse']['user'], password = config['clickhouse']['password'])

    launches = clickhouse.query_dataframe('select distinct item_id, name, urls, is_oss from hn_launches.launches;')

    #[Get GitHub stats]
    github_api_token = config['github']['api_key']

    company_oss = pd.DataFrame(columns = ['item_id', 'github_repo', 'github_stars', 'github_forks', 'github_open_issues'])

    for i, row in launches[launches['is_oss'] == 1].iterrows():

        if len([u for u in row['urls'] if 'github' in u]) > 0:
            repo = [u.split('.com/')[-1].replace('/releases/latest', '') for u in row['urls'] if 'github' in u][-1].split(',')[0]

            if row['name'].lower() in repo:
                response = r.get(f'https://api.github.com/repos/{repo}',
                                 headers = {'Authorization': f'token {github_api_token}'})


                if 'Bad credentials' in response.text:
                    raise Exception('Github API-token is dead')

                if 'Not Found' not in response.text:
                    response = json.loads(response.text)

                    company_oss = company_oss.append({
                        'item_id': row['item_id'], 
                        'github_repo': repo, 
                        'github_stars': response['stargazers_count'],
                        'github_forks': response['forks_count'],
                        'github_open_issues': response['open_issues_count']},
                    ignore_index = True)

            company_oss.append(company_oss)

    company_oss['item_id'] = company_oss['item_id'].astype('int')

    context.log.info(f"Parsed {len(company_oss['item_id'])} GitHub repos")

    return company_oss
