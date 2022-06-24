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


@op(description = 'Match Growjo and Github data and load to CH')
def transform_load_growth(context, company_growth:pd.core.frame.DataFrame, company_oss:pd.core.frame.DataFrame) -> None:

    clickhouse = Client(host = config['clickhouse']['host'], database = 'hn_launches',
                        user = config['clickhouse']['user'], password = config['clickhouse']['password'])

    ch_company_growth = ('select distinct item_id from hn_launches.company_growth;')

    company_growth = pd.merge(company_growth, company_oss, how = 'outer', on = 'item_id').replace(np.nan, 0)
    
    company_growth = company_growth.astype({'founded': 'int', 'employees': 'int', 'estimated_revenue': 'int', 'total_funding': 'float',
                                            'github_stars': 'int', 'github_forks': 'int', 'github_open_issues': 'int'})
    
    
    if len(company_growth['item_id']) >= len(ch_company_growth['item_id']):

        clickhouse.execute('alter table hn_launches.company_growth delete where 1=1;')
        
        rows = clickhouse.execute('insert into hn_launches.company_growth values',
                                  company_growth.to_dict('records'), types_check = True)
        
        context.log.info(f"Companies growth loaded to CH, inserted rows: {rows}")

