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


@op(description = 'Parse companies growth info from Gowjo')
def parse_load_growjo(context) -> None:
    
    clickhouse = Client('85.193.83.20', database = 'hn_launches',
                        user = 'admin', password = '0987654321')
    
    launches = clickhouse.query_dataframe('select distinct item_id, name, urls, is_oss from hn_launches.launches;')
    ch_company_growth = ('select distinct item_id from hn_launches.company_growth;')
    
    
    #[Parse Growjo companies metrics]
    company_growth = pd.DataFrame(columns = ['item_id', 'founded', 'url', 'linkedin_url', 'country',  
                                             'employees', 'estimated_revenue', 'total_funding'])

    for name in launches['name'].unique():

        time.sleep(np.random.poisson(5))

        response = r.get(f"https://growjo.com/api/companies/{name.replace(' ', '_')}", 
                         headers = {'authority': 'growjo.com', \
         'accept': 'application/json, text/plain, */*', \
         'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8', \
         'auth': 'Basic Z3Jvd2pvQXBpVXNlcjpqazYhNVo5UHViQi5Idlo=', \
         'cookie': 'ezoadgid_333975=-1; ezoref_333975=google.com; ezepvv=0; lp_333975=https://growjo.com/; ezosuibasgeneris-1=042958e8-fecf-4431-6dfd-89f2f0a4cc6a; ezoab_333975=mod55; ezovid_333975=1843953631; ezovuuid_333975=41875372-733e-45f5-4565-7547dd0c616c; _ga=GA1.2.1069300794.1655469772; _gid=GA1.2.980828187.1655469772; ezouspvv=0; ezouspva=0; landingPage=/; ezds=ffid%3D1%2Cw%3D1440%2Ch%3D900; ezohw=w%3D1440%2Ch%3D711; __qca=P0-42128702-1655469773770; ezux_lpl_333975=1655469773917|dcbbd6da-5b8a-4797-5fd5-52d9002ab804|false; ezux_ifep_333975=true; ezux_et_333975=116; ezux_tos_333975=168; active_template::333975=pub_site.1655469946; ezopvc_333975=3; ezovuuidtime_333975=1655469947', \
         'referer': f"https://growjo.com/company/{name.replace(' ', '%20')}", \
         'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"', \
         'sec-ch-ua-mobile': '?0', \
         'sec-ch-ua-platform': '"macOS"', \
         'sec-fetch-dest': 'empty', \
         'sec-fetch-mode': 'cors', \
         'sec-fetch-site': 'same-origin', \
         'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'})

        if response.status_code == 200:
            if 'errorCode' not in response.text:
                response = json.loads(response.text)

                company_growth = company_growth.append({
                    'item_id': launches[launches['name'] == name]['item_id'], 
                    'founded': response['founded'],
                    'url':response['url'], 
                    'linkedin_url': response['linkedin_url'], 
                    'country': response['country'], 
                    'employees': response['current_employees'], 
                    'estimated_revenue': response['estimated_revenues'], 
                    'total_funding': response['total_funding_float']},
                    ignore_index = True)

        company_growth.append(company_growth)

    company_growth['item_id'] = company_growth['item_id'].astype('int')
    
    context.log.info(f"Parsed {len(company_growth['item_id'])} Growjo companies")
    
    
    #[Get GitHub stats]
    github_api_token = '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'

    company_oss = pd.DataFrame(columns = ['item_id', 'github_repo', 'github_stars', 'github_forks', 'github_open_issues'])

    for i, row in launches[launches['is_oss'] == 1].iterrows():

        if len([u for u in row['urls'] if 'github' in u]) > 0:
            repo = [u.split('.com/')[-1].replace('/releases/latest', '') for u in row['urls'] if 'github' in u][-1].split(',')[0]

            if row['name'].lower() in repo:
                response = r.get(f'https://api.github.com/repos/{repo}',
                                 headers = {'Authorization': f'token {github_api_token}'})

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
    
    company_growth = pd.merge(company_growth, company_oss, how = 'outer', on = 'item_id').replace(np.nan, 0)
    
    company_growth = company_growth.astype({'founded': 'int', 'employees': 'int', 'estimated_revenue': 'int', 'total_funding': 'float',
                                            'github_stars': 'int', 'github_forks': 'int', 'github_open_issues': 'int'})
    
    
    if len(company_growth['item_id']) >= len(ch_company_growth['item_id']):
        
        clickhouse.execute('alter table hn_launches.company_growth delete where 1=1;')
        
        rows = clickhouse.execute('insert into hn_launches.company_growth values',
                                  company_growth.to_dict('records'), types_check = True)
        
        context.log.info(f"Companies growth loaded to CH, inserted rows: {rows}")

    
    