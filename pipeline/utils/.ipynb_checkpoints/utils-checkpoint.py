import os, sys, time, re, random
import datetime as dt
import numpy as np
import pandas as pd
import requests as r 
import json


headers__list = \
    [{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
      'Accept-Encoding': 'gzip, deflate, br', 
      'Accept-Language': 'en-US,en;q=0.9',},
     {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
      'Accept-Encoding': 'gzip, deflate, br', 
      'Accept-Language': 'en-US,en;q=0.9',},
     {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
      'Accept-Encoding': 'gzip, deflate, br', 
      'Accept-Language': 'en-GB,en;q=0.9',},
     {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15; rv:100.0) Gecko/20100101 Firefox/100.0',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
      'Accept-Encoding': 'gzip, deflate, br', 
      'Accept-Language': 'en-GB,en;q=0.9',},]

history = ['http://google.com', 'https://www.google.com/search?q=hacker+news', 'https://news.ycombinator.com/news']


def create_session():
    
    global s
    if 's' in globals(): del s
    
    session = r.Session()
    session.headers.update(random.choice(headers__list))
    [session.get(i) for i in history]
    
    return session

#def rotate_ip()

labels__dict = {'Healthcare': ['health ', 'telehealth', 'telemedicine', 'wellness', 'fitness', 'doctor', 'patients', 'meds', 'drugs', 'hospitalization', \
                               'therapy', 'diseases', 'psychology', 'sleep', 'meditation', 'relief', 'medication'],
                'Education': ['education', 'teaching', 'student', 'teacher', ' class ', 'exam', 'schooler'],
                'Food': ['meat', 'food', 'restaurant'],
                'Green': ['co2', 'carbon'],
                'Gaming': ['game', 'gaming'],
                'AR&VR': ['3d', ' ar ', ' vr '],
                'Crypto': ['cryptocurrency', 'crypto', 'dao', 'nft', 'web 3.0'],
                'Financial': ['card', 'bank', 'digital bank', 'neo-bank', 'loan', 'debt', 'venture', 'financicals', \
                              'trading', 'trade', 'portfolio', 'investment', 'investing', 'invest', 'stocks', 'pay', 'bill'],
                'Industrials': ['industrial', 'drones', 'aircraft', 'aviation', ' space ', 'robotics', 'manufacturing', 'energy', 'agro', 'industrials', \
                                'facilities', 'systems', 'oil', 'gas', ' solar '],
                'SaaS': ['as a service', 'company', 'b2b', 'smb', 'saas', 'billing', 'subscription', 'marketing', 'retail', 'sale', 'hr ', 'hire', \
                         'platform', 'onboarding', 'automation', 'automate', 'marketplaces', 'builder', 'co-working', 'workers', 'for companies', \
                         'shopify', 'selling', 'no-code', 'optimization', 'workspace', 'remote', 'employee', 'build', 'create', 'collaborative', \
                         'e-commerce', 'email', ' docs', 'product', 'for founders', 'for startups', 'support', 'customer', 'collaboration', 'authentication', \
                         'office', 'meeting'],
                'Consumer': ['personal', ' men ', ' women ', 'adults', 'teens', 'kids', 'for people', 'apparel', 'social', 'dating', 'planning', 'people', 'apparel', \
                             'booking', 'at-home'],
                'DevTools': ['design', 'coding', ' code', 'developer', ' ide ', 'deploy', 'cloud', 'data', ' prod', 'database', 'pipelines', 'etl ', 'k8s', 'aws ', 'kubernetes', \
                             'ci/cd', ' ml ', 'engineering', 'full-stack', 'fullstack', 'data analytics', 'data science', 'open-source', 'open source', \
                             'microservices', 'server', 'incidents', 'debug', 'secrets', 'securely', 'security', 'front-end', 'back-end', 'machine learning'],} 