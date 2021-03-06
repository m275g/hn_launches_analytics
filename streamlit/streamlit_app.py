import streamlit as st
import datetime as dt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import clickhouse_driver
from clickhouse_driver.client import Client


#[Params]
st.set_page_config(
    page_title = 'HN Laucnhes',
    page_icon = '🌱',
    layout = 'wide')


#[Define funcs]
@st.cache
def get_launches_metrics(): 
    
    clickhouse = Client(host = st.secrets['clickhouse']['host'], database = 'hn_launches',
                        user = st.secrets['clickhouse']['user'], password =  st.secrets['clickhouse']['password'])

    metrics = clickhouse.query_dataframe("""
    select l.item_id as item_id, max(date(l.time)) as date, max(name) as name, 
    max(yc_batch) as yc_batch, 
    toInt8(max(replaceRegexpAll(l.yc_batch, 'S|W| Nonprofit', ''))) as yc_batch_y,
    max(short_description) as short_description, 
    max(industry) as industry, max(is_oss) as is_oss,
    max(length(text)) as text_len,
    max(score) as score,
    uniq(comment_id) as comments_qty,
    uniqIf(comment_id, sentiment == 'pos') as comments_pos_qty,
    uniqIf(comment_id, sentiment == 'neg') as comments_neg_qty,
    uniqIf(comment_id, sentiment == 'neu') as comments_neu_qty,
    max(employees) as employees, max(estimated_revenue) as revenue, max(total_funding) as total_funding, max(github_stars) as github_stars
    from hn_launches.launches l
    left join hn_launches.comments c on c.item_id = l.item_id
    left join hn_launches.company_growth g on g.item_id = l.item_id
    group by item_id;
    """)

    metrics['date'] = pd.to_datetime(metrics['date'])
    
    return metrics


#[Get data from Clickhouse]
try:
    launches_metrics = get_launches_metrics()
except Exception:
    st.error('Data is not available at the moment:( Please, try later.')
    #st.exception(Exception)
    st.stop()
    
#[Const]
metrics = ['text_len', 'score', 'comments_qty', 'comments_pos_qty', 'comments_neg_qty', 'comments_neu_qty', 'employees', 'revenue', 'total_funding', 'github_stars']

industries = launches_metrics['industry'].unique().tolist()
yc_batches = launches_metrics['yc_batch'].unique().tolist()
names = launches_metrics['name'].unique().tolist()


#[App]
#Description
st.title("YC Companies' launches in HackerNews")
st.text(
"""
HackerNews has a Launches page where YC companies can describe their launch.
This is analytics of YC Companies' launches in HackerNews: 
- launch metrics like score, comments qty, etc
- top commented/scored industries & launches, 
- top launch by selected metric
- correlation between launch metrics and company growth metrics
- more...
by: @m275g
""")

#Launches dynamics
st.subheader('Launches dynamics')
launches_qty__col, avg_score__col, avg_comments__col = st.columns(3)

#Launches stats
with launches_qty__col:
    st.metric('Launches Qty', launches_metrics['item_id'].nunique())
    
with avg_score__col:
    st.metric('Avg Score on Launch', launches_metrics['score'].mean().round(2))
    
with avg_comments__col:
    st.metric('Avg Comments on Launch', launches_metrics['comments_qty'].mean().round(2))

#Launches line
st.plotly_chart(\
px.line(launches_metrics[launches_metrics['yc_batch_y'].isin([19, 20, 21])] \
            .groupby(pd.Grouper(key = 'date', freq = 'M', axis = 0))['item_id'] \
            .nunique().reset_index().rename(columns = {'item_id': 'launches'}), 
        x = 'date', y = 'launches', 
        title = 'Launches Dynamics').update_traces(line_color = '#f26522'), use_container_width = True)

#Batch
launches_bar__col, launches_tree__col = st.columns(2)

with launches_bar__col:
    st.plotly_chart(\
    px.bar(launches_metrics[(launches_metrics['yc_batch_y'].isin([19, 20, 21])) & \
                            (~launches_metrics['yc_batch'].str.contains('Nonprofit'))] \
               .groupby(['yc_batch', 'yc_batch_y', 'industry'])['item_id'] \
               .nunique().reset_index().sort_values('yc_batch_y', ascending = False).rename(columns = {'item_id': 'launches'}), 
           x = 'launches', y = 'yc_batch', color = 'industry',
           title = 'Launches by Batches'), use_container_width = True)

with launches_tree__col:
    st.plotly_chart(\
    px.treemap(launches_metrics[(launches_metrics['yc_batch_y'].isin([19, 20, 21])) & \
                                (~launches_metrics['yc_batch'].str.contains('Nonprofit'))] \
                   .groupby(['yc_batch', 'industry'])['item_id'] \
                   .nunique().reset_index().rename(columns = {'item_id': 'launches'}), 
               values = 'launches', path = ['yc_batch', 'industry'], 
               title = 'Launches by Batch&Industry'), use_container_width = True)

#Industries tops
st.subheader('Top Industries by score&comments')
ind_top_score__col, ind_top_comm__col = st.columns(2)

#Industries top by score
with ind_top_score__col:
    st.plotly_chart(\
    px.bar(launches_metrics.groupby(['industry'])['score'].mean().reset_index() \
               .sort_values('score', ascending = False),
           x = 'score', y = 'industry', color = 'industry', 
           title = 'Top Scored Industries').update(layout_showlegend = False), use_container_width = True)

#Industries top by comments_qty
with ind_top_comm__col:
    st.plotly_chart(\
    px.bar(launches_metrics.groupby(['industry'])['comments_qty'].mean().reset_index() \
               .sort_values('comments_qty', ascending = False),
           x = 'comments_qty', y = 'industry', color = 'industry', 
           title = 'Top Commented Industries').update(layout_showlegend = False), use_container_width = True)

#Industries top by pos/neg comments
st.subheader('Top Industries by negative&positive comments')
ind_top_neg__col, ind_top_pos__col = st.columns(2)

#Industries top by comments_neg_qty
with ind_top_neg__col:
    st.plotly_chart(\
    px.bar(launches_metrics.groupby(['industry'])['comments_neg_qty'].mean().reset_index() \
               .sort_values('comments_neg_qty', ascending = False),
           x = 'comments_neg_qty', y = 'industry', color = 'industry', 
           title = 'Top Negative Commented Industries') \
      .update_xaxes(autorange = 'reversed').update(layout_showlegend = False).update_traces(marker_color = '#ef553b'), use_container_width = True)

#Industries top by comments_pos_qty
with ind_top_pos__col:
    st.plotly_chart(\
    px.bar(launches_metrics.groupby(['industry'])['comments_pos_qty'].mean().reset_index() \
               .sort_values('comments_pos_qty', ascending = False),
           x = 'comments_pos_qty', y = 'industry', color = 'industry', 
           title = 'Top Positive Commented Industries') \
      .update(layout_showlegend = False).update_traces(marker_color = '#f26522'), use_container_width = True)

#Top Launches by comments
st.subheader('Top Launches by comments')
st.text('Top 100 Launches by comments qty')
st.plotly_chart(\
px.bar(launches_metrics.groupby(['name']) \
       [['comments_qty', 'comments_neu_qty', 'comments_pos_qty', 'comments_neg_qty']].max().reset_index() \
           .sort_values('comments_qty', ascending = False)[:100],
       y = ['comments_neu_qty', 'comments_pos_qty', 'comments_neg_qty'], x = 'name', 
       color_discrete_map = {'comments_neu_qty': '#636EFA', 'comments_pos_qty': '#00CC96', 'comments_neg_qty': '#EF553B'},
       title = f'Top Commented Lauhches'), use_container_width = True)

#Launhes top by pos/neg comments
st.subheader('Top Launches by negative&positive comments')
st.text('Top 50 Launches by negative&positive comments qty')
launch_top_neg__col, launch_top_pos__col = st.columns(2)

#Launhes top 50 by comments_neg_qty
with launch_top_neg__col:
    st.plotly_chart(\
    px.bar(launches_metrics.groupby(['name'])['comments_neg_qty'].sum().reset_index() \
               .sort_values('comments_neg_qty', ascending = False)[:50],
           x = 'comments_neg_qty', y = 'name', 
           title = 'Top Negative Commented Launches') \
      .update_xaxes(autorange = 'reversed').update(layout_showlegend = False).update_traces(marker_color = '#ef553b'), use_container_width = True)

#Launhes top 50 by comments_pos_qty
with launch_top_pos__col:
    st.plotly_chart(\
    px.bar(launches_metrics.groupby(['name'])['comments_pos_qty'].sum().reset_index() \
               .sort_values('comments_pos_qty', ascending = False)[:50],
           x = 'comments_pos_qty', y = 'name', 
           title = 'Top Positive Commented Launches') \
      .update(layout_showlegend = False).update_traces(marker_color = '#f26522'), use_container_width = True)

#Top laucnhes by {metric} bar
st.subheader('Top Launches by Metric')
st.text('Top 100 Launches by selected metric')
metric = st.selectbox('Metric', ('text_len', 'score', 'comments_qty', 'comments_pos_qty', 'comments_neg_qty', 'comments_neu_qty', 'employees', 'revenue', 'total_funding', 'github_stars'))

st.plotly_chart(\
px.bar(launches_metrics.groupby(['name'])[metric].max().reset_index().sort_values(metric, ascending = False)[:100],
       y = metric, x = 'name', 
       title = f'Top Launches by {metric}').update_traces(marker_color = '#f26522'), use_container_width = True)

#Metrics scatterplot
st.subheader('Metric vs Other Metric')
st.text(
"""
This graph shows correlation between two metrics
Select two metrics (MetricX and MetricY) and point size - MetricSize
""")
with st.container():
    
    metric_x__col, metric_y__col, metric_size__col = st.columns(3)
    
    with metric_x__col:
        metric_x = st.selectbox('Metric X', metrics, 1)
        
    with metric_y__col:
        metric_y = st.selectbox('Metric Y', metrics, 2)
    
    with metric_x__col:
        metric_size = st.selectbox('Metric Size', metrics, 0)
    
st.plotly_chart(\
px.scatter(launches_metrics, x = metric_x, y = metric_y, color = 'industry', size = metric_size, 
           height = 600,
           title = f'{metric_x} vs {metric_y}, size: {metric_size}'), use_container_width = True)

#corr 
corr = launches_metrics[['text_len', 'score',
                         'comments_qty', 'comments_pos_qty', 'comments_neg_qty', 
                         'employees', 'revenue', 'total_funding', 'github_stars']].corr().round(2)

#Impact to score
st.subheader('Metrics impact to Score')
st.text(
"""
This is impact level of metrics to score
""")
st.plotly_chart(\
px.bar(corr['score'][['text_len', 'comments_qty', 'comments_pos_qty', 'comments_neg_qty',
                      'employees', 'revenue', 'total_funding', 'github_stars']].to_frame().reset_index() \
           .rename(columns = {'score': 'corr'}).sort_values('corr', ascending = False), 
       x = 'index', y = 'corr', range_y = [-0.5, 1], orientation = 'v', 
       title = 'Metrics impact to score').update_traces(marker_color = '#f26522'), use_container_width = True)

#Correlation map
st.subheader('Metrics Correlation map')
st.text(
"""
This is Pearson correlaion map between metrics
""")
st.plotly_chart(\
go.Figure(go.Heatmap(z = corr.values, x = corr.columns, y = corr.columns, 
                     text = corr.values, texttemplate = '%{text}',
                     colorscale = px.colors.diverging.RdBu,
                     zmin = -1, zmax = 1)), use_container_width = True)

#Search for launches
st.subheader('Search for Launches')
st.text('Launches table with filtering')
with st.container():
        
    name__col, yc_batch__col, industry__col, is_oss__col = st.columns(4)
    
    with name__col:
        name = st.text_input('Name')
        
    with yc_batch__col:
        yc_batch = st.multiselect('YC Batch', yc_batches)
    
    with industry__col:
        industry = st.multiselect('Industry', industries)
        
    with is_oss__col:
        is_oss = st.checkbox('Is Open-Source')
        
    if not yc_batch: yc_batch = yc_batches
    if not industry: industry = industries
    
launches_table = launches_metrics[(launches_metrics['name'].str.contains(name, case = False)) & \
                                  (launches_metrics['yc_batch'].isin(yc_batch)) & \
                                  (launches_metrics['industry'].isin(industry)) & \
                                  (launches_metrics['is_oss'].isin(industry) == int(is_oss))][['name', 'yc_batch', 'industry', 'is_oss', 'short_description', 'score', 'comments_qty', 'employees', 'revenue', 'total_funding', 'github_stars']]

launches_table[['employees', 'revenue', 'total_funding']] = launches_table[['employees', 'revenue', 'total_funding']].replace([0, 0.0], 'NA').astype('str')
launches_table['github_stars'] = launches_table['github_stars'].replace(0, '').astype('str')

st.dataframe(launches_table.style.set_properties(subset = ['is_oss'], **{'width': '1000px'}))


st.text(
"""

Lauches posts from news.ycombinator.com/launches
Companies growth-metrics from growjo.com
Pipeline on Dagster
Data stored in Clickhouse
Repo: github.com/m275g/hn_launches_analytics/tree/main/streamlit
""")