# HackerNews Launches Analytics
Analytics of YC companies' launches on HackerNews  
Parsing launches posts from news.ycombinator.com/launches  
Companies growth-metrics parsing from growjo.com  
For OSS products get GitHub statistics

- Extracts data fields from launches posts, such as: 
  - company name, yc_batch, industry, short_description
  - is the product OSS
- Sentiment analysis of comments to launches
- Parsing company growth-metrics from growjo.com, such as:
  - revenue, employees, funding
- Parsing OSS products GitHub statistics: stars, forks  
- Dashboard with launches analytics on Streamlit

Data processing is placed in a DAG-system - Dagster.io

### Pipeline
Pipeline on Dagster  

### Streamlit Dashbord
This is analytics of YC Companies' launches in HackerNews: 
- launch metrics like score, comments qty, etc
- top commented/scored industries & launches, 
- top launch by selected metric
- correlation between launch metrics and company growth metrics
- more...  

Deployed dashboard on Streamlit.Share: https://m275g-hn-launches-analytics-streamlitstreamlit-app-dp1i7o.streamlitapp.com/
