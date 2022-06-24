import os, sys, time, random
from typing import Tuple
import yaml
from yaml.loader import SafeLoader
import datetime as dt
import numpy as np
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import clickhouse_driver
from clickhouse_driver.client import Client
from utils.utils import *
from dagster import op, Out, In


with open('config.yaml') as c: 
    config = yaml.load(c, Loader = SafeLoader)


@op(description = 'Tranform HN launches: fields extraction, labeling, comments sentiments; and load to CH')
def transform_load_hn_launches(context, launches_dict:dict) -> None:
    
    launches_table = launches_dict['launches_table']
    launches_pages = launches_dict['launches_pages']
    launches_comments = launches_dict['launches_comments']
    
        
    #[Сhecking if parsed new items]
    clickhouse = Client(config['clickhouse']['host'], database = 'hn_launches',
                        user = config['clickhouse']['user'], config['clickhouse']['password'])
    
    ch_launches = clickhouse.query_dataframe('select distinct item_id from hn_launches.launches;')
    ch_comments = clickhouse.query_dataframe('select distinct comment_id from hn_launches.comments;')
    
    new_launches = list(set(launches_pages['item_id']) - set(ch_launches['item_id']))
    new_comments = list(set(launches_comments['comment_id']) - set(ch_comments['comment_id']))
        
    context.log.info(f"Parsed {len(new_launches)} new HN launches")
    context.log.info(f"Parsed {len(new_comments)} new HN comments")
    
    if new_launches:
    
        launches = pd.merge(launches_table, launches_pages, how = 'inner', on = 'item_id')
        launches = launches[launches['item_id'].isin(new_launches)]
        #del launches_id, launches_text


        #[Launches clearing]
        launches = launches[~launches['title'].str.contains('meet the batch', case = False) &
                            ~launches['text'].str.contains('meet the batch', case = False)].reset_index(drop = True)

        #[Fields extraction]
        launches['name'] = launches.apply(lambda row: re.search('Launch HN: (.*?) \(YC', row['title']).group(1), axis = 1)
        launches['yc_batch'] = launches.apply(lambda row: re.search('\(YC (.*?)\)', row['title']).group(1), axis = 1)
        launches['short_description'] = launches.apply(lambda row: re.search('\)(.*)', row['title']).group(1) \
                                                       .replace(' – ', '').replace('- ', '').replace(': ', '').capitalize(), axis = 1)
        #launches['is_oss'] = (launches['title'].str.contains('open-source|oss|open source', case = False)).astype('int')
        launches['is_oss'] = launches.apply(lambda row: 1 if (((bool(re.search('open-source|open source', row['text']))) \
                                                               & (bool(re.search('github.com', row['text'])))) \
                                                              or (bool(re.search('open-source|open source', row['short_description'])))) \
                                                          else 0, axis = 1)
        launches['urls'] = launches.apply(lambda row: ['https://'+u for u in re.findall('\(https://(.*?)\)', row['text'])], axis = 1)

        launches = launches[['item_id', 'by', 'time', 'name', 'yc_batch', 'short_description', 'is_oss', 'urls', 'url', \
                             'text', \
                             'score']]


        #[Launches labeling]
        conditions = [launches['short_description'].str.contains('|'.join(item), case = False) for item in labels__dict.values()]
        values = [item for item in labels__dict]

        launches['industry'] = np.select(conditions, values, default = 'Other')
        
        rows = clickhouse.execute('insert into hn_launches.launches values',
                                  launches.to_dict('records'), types_check = True)
        
        context.log.info(f"HN launches loaded to CH, inserted rows: {rows}")
    
    
    #[Comments sentiments]
    if new_comments:
       
        comments = launches_comments[launches_comments['comment_id'].isin(new_comments)]
    
        sia = SentimentIntensityAnalyzer()

        def analyze_sentiment(text):

            scores = sia.polarity_scores(text)

            if (scores['pos'] > 0.1) and (scores['neg'] < 0.1): sentiment = 'pos'
            elif (scores['pos'] < 0.1) and (scores['neg'] > 0.1): sentiment = 'neg'
            else: sentiment = 'neu'

            return sentiment

        comments['sentiment'] = comments.apply(lambda row: analyze_sentiment(row['comment']), axis = 1)
        
        rows = clickhouse.execute('insert into hn_launches.comments values',
                                  comments.to_dict('records'), types_check = True)
        
        context.log.info(f"HN comments loaded to CH, inserted rows: {rows}")