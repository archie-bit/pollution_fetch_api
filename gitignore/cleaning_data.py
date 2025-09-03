#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json

#------------CLEANING WEATHER FILE------------#

with open('weather_data.json', 'r') as file:
    data = json.load(file)

records = []
for city,instances in data.items():
    for city_info in instances:
        city_info = city_info['data']
        flat = pd.json_normalize(city_info, sep='_').to_dict(orient='records')[0]
        flat['city'] = city
        
        weather_info = pd.json_normalize(city_info['weather'], sep='_').to_dict(orient='records')[0]
        for key, value in weather_info.items():
            flat[f'weather_{key}'] = value
        flat.pop('weather', None)
        records.append(flat)

df_weather = pd.DataFrame(records)
# df_weather.set_index('city', inplace=True)
df_weather['dt'] = pd.to_datetime(df_weather['dt'], unit='s')
df_weather['sys_sunrise'] = pd.to_datetime(df_weather['sys_sunrise'], unit='s')
df_weather['sys_sunset'] = pd.to_datetime(df_weather['sys_sunset'], unit='s')

df_weather


# In[2]:


#------------CLEANING Pollution FILE------------#

with open('pollution_data.json') as file:
    data = json.load(file)

pollution_records = []
for city, instances in data.items():
        for city_poll in instances:
            city_poll= city_poll['data']
            city_poll_flat = pd.json_normalize(city_poll, sep='_').to_dict(orient='records')[0]
            city_poll_flat['city'] = city

            pollution_info = pd.json_normalize(city_poll_flat['list'], sep="_").to_dict(orient='records')[0]
            for key, value in pollution_info.items():
                city_poll_flat[f'pollution_{key}'] = value
            city_poll_flat.pop('list', None)

            pollution_records.append(city_poll_flat)

df_pollution = pd.DataFrame(pollution_records)
# df_pollution.set_index('city', inplace=True)
df_pollution['pollution_dt'] = pd.to_datetime(df_pollution['pollution_dt'], unit='s')

df_pollution


# In[3]:


try:
    df_pollution.drop(columns=['coord_lon', 'coord_lat'], inplace=True)
except:
    pass
df_merged= df_weather.merge(
    df_pollution,
    on='city',
    how='inner'
)
df_merged.set_index('city', inplace=True)
df_merged.drop_duplicates(inplace=True)
df_merged


# In[4]:


#Checking dups
print(df_merged.duplicated().sum())

#Checking NA
print(df_merged.isna().any())

#Since wind gust has high probability of being null and doesn't contribute
#much in the analysis we can drop it
df_merged.dropna(axis=1, inplace=True)
print(df_merged.isna().any())


# In[5]:


df_merged.info()


# In[6]:


date_dim = pd.concat([
    df_merged['pollution_dt'],
    df_merged['dt'],
    df_merged['sys_sunrise'],
    df_merged['sys_sunset']
], ignore_index=True)
date_dim = pd.to_datetime(date_dim.dropna().unique())
date_dim = pd.DataFrame({'date_time': date_dim})
date_dim['date_time'] = date_dim['date_time'].dt.floor('H')
date_dim['date'] = date_dim['date_time'].dt.date
date_dim['year'] = date_dim['date_time'].dt.year
date_dim['month'] = date_dim['date_time'].dt.month
date_dim['day'] = date_dim['date_time'].dt.day
date_dim['weekday'] = date_dim['date_time'].dt.day_name()
date_dim['hour'] = date_dim['date_time'].dt.hour
date_dim['is_weekend'] = date_dim['weekday'].isin(['Saturday', 'Sunday'])
date_dim = date_dim.drop_duplicates().reset_index(drop=True)
date_dim['date_sk'] = date_dim.index
cols = ['date_sk']+[c for c in date_dim.columns if c!= 'date_sk']
date_dim = date_dim[cols]
date_dim.to_csv('date_dim.csv', index=False)


# In[7]:


weather_dim = df_merged[['weather_id', 'weather_main', 'weather_description', 'weather_icon']].drop_duplicates().reset_index(drop=True)
weather_dim['weather_sk']= weather_dim.index

cols = ['weather_sk']+[c for c in weather_dim.columns if c!= 'weather_sk']
weather_dim = weather_dim[cols]
weather_dim.to_csv('weather_dim.csv', index=False)

weather_dim


# In[8]:


location_dim = df_merged.reset_index()[['city', 'sys_country', 'coord_lon', 'coord_lat', 'timezone','base']].drop_duplicates().reset_index(drop=True)
location_dim['location_sk']=location_dim.index


cols = ['location_sk']+[c for c in location_dim.columns if c not in ['location_sk']]
location_dim = location_dim[cols]
location_dim.to_csv('location_dim.csv', index=False)
location_dim


# In[9]:


pollutants_fact = pd.concat([
    df_merged.filter(regex='^pollution'),
    df_merged[['weather_id']]
    ], axis = 1).drop_duplicates().reset_index()
pollutants_fact['pollutants_sk'] = pollutants_fact.index


pollutants_fact['pollution_dt'] = pollutants_fact['pollution_dt'].dt.floor('H')

pollutants_fact = pollutants_fact.merge(
    date_dim[['date_sk', 'date_time']],
    left_on= 'pollution_dt',
    right_on='date_time',
    how='left'
).merge(
    location_dim[['city', 'location_sk']],
    on='city',
    how='left',
).merge(
    weather_dim[['weather_id', 'weather_sk']],
    on='weather_id',
    how='left'
).drop(columns=['pollution_dt', 'weather_id', 'date_time', 'city'])


cols = ['pollutants_sk', 'location_sk', 'weather_sk','date_sk']+[c for c in pollutants_fact.columns if c not in ['pollutants_sk', 'date_sk', 'location_sk', 'weather_sk']]
pollutants_fact = pollutants_fact[cols]
pollutants_fact.rename(columns=lambda col: col.replace('pollution_', '') if col.startswith('pollution_') else col, inplace=True)
pollutants_fact.to_csv('pollutants_fact.csv', index=False)
pollutants_fact


# In[10]:


weather_fact = pd.concat([
    df_merged.filter(regex='^main'),
    df_merged[['sys_sunrise', 'sys_sunset', 'visibility', 'dt', 'weather_id']]
], axis=1).drop_duplicates().reset_index()

weather_fact['weather_fact_sk'] = weather_fact.index

weather_fact['sys_sunrise'] = weather_fact['sys_sunrise'].dt.floor('H')
weather_fact= weather_fact.merge(
    date_dim[['date_sk', 'date_time']],
    left_on='sys_sunrise',
    right_on='date_time',
    how='left',
).drop(columns=['date_time','sys_sunrise' ])
weather_fact.rename(columns={"date_sk":'sunrise_date_sk'}, inplace=True)

weather_fact['sys_sunset'] = weather_fact['sys_sunset'].dt.floor('H')
weather_fact= weather_fact.merge(
    date_dim[['date_sk', 'date_time']],
    left_on='sys_sunset',
    right_on='date_time',
    how='left',
).drop(columns=['date_time','sys_sunset' ])
weather_fact.rename(columns={"date_sk":'sunset_date_sk'}, inplace=True)
# weather_fact['sys_sunset'] = weather_fact['sys_sunset'].dt.floor('H')

weather_fact['dt'] = weather_fact['dt'].dt.floor('H')
weather_fact= weather_fact.merge(
    date_dim[['date_sk', 'date_time']],
    left_on='dt',
    right_on='date_time',
    how='left',
).drop(columns=['date_time','dt'])

weather_fact= weather_fact.merge(
    location_dim[['city', 'location_sk']],
    on='city',
    how='left'
).drop(columns=['city'])

weather_fact= weather_fact.merge(
    weather_dim[['weather_id', 'weather_sk']],
    on='weather_id',
    how='left'
).drop(columns=['weather_id'])

cols= ['weather_fact_sk', 'weather_sk','location_sk','date_sk' ,'sunrise_date_sk', 'sunset_date_sk'] + [c for c in weather_fact.columns if c not in ['weather_fact_sk', 'location_sk', 'sunrise_date_sk', 'sunset_date_sk', 'date_sk', 'weather_sk']]
weather_fact = weather_fact[cols]
weather_dim.rename(columns=lambda col: col.replace('main_', '') if col.startswith('main_') else col, inplace=True )
weather_fact.to_csv('weather_fact.csv', index=False)
weather_fact

