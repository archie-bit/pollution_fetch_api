import pandas as pd
import json
import os

DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

WEATHER_FILE = os.path.join(DATA_DIR, "weather_data.json")
POLLUTION_FILE = os.path.join(DATA_DIR, "pollution_data.json")

DATE_DIM_FILE = os.path.join(DATA_DIR, "date_dim.csv")
WEATHER_DIM_FILE = os.path.join(DATA_DIR, "weather_dim.csv")
LOCATION_DIM_FILE = os.path.join(DATA_DIR, "location_dim.csv")
POLLUTANTS_FACT_FILE = os.path.join(DATA_DIR, "pollutants_fact.csv")
WEATHER_FACT_FILE = os.path.join(DATA_DIR, "weather_fact.csv")

def data_cleaning():

    with open(WEATHER_FILE, 'r') as file:
        data = json.load(file)

    weather_records = []
    for city, instances in data.items():
        for city_info in instances:
            city_info = city_info['data']
            flat = pd.json_normalize(city_info, sep='_').to_dict(orient='records')[0]
            flat['city'] = city
            weather_info = pd.json_normalize(city_info['weather'], sep='_').to_dict(orient='records')[0]
            for k, v in weather_info.items():
                flat[f'weather_{k}'] = v
            flat.pop('weather', None)
            weather_records.append(flat)

    df_weather = pd.DataFrame(weather_records)
    df_weather['dt'] = pd.to_datetime(df_weather['dt'], unit='s')
    df_weather['sys_sunrise'] = pd.to_datetime(df_weather['sys_sunrise'], unit='s')
    df_weather['sys_sunset'] = pd.to_datetime(df_weather['sys_sunset'], unit='s')

    with open(POLLUTION_FILE, 'r') as file:
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
    df_pollution['pollution_dt'] = pd.to_datetime(df_pollution['pollution_dt'], unit='s')

    try:
        df_pollution.drop(columns=['coord_lon', 'coord_lat'], inplace=True)
    except:
        pass

    df_merged = df_weather.merge(df_pollution, on='city', how='inner').drop_duplicates().reset_index(drop=True)

    date_times = pd.concat([
        df_merged['pollution_dt'],
        df_merged['dt'],
        df_merged['sys_sunrise'],
        df_merged['sys_sunset']
    ], ignore_index=True)
    date_times = pd.to_datetime(date_times.dropna().unique())
    date_dim_new = pd.DataFrame({'date_time': date_times})     
    date_dim_new['date_time'] = date_dim_new['date_time'].dt.floor('H')
    date_dim_new = date_dim_new.drop_duplicates()
    date_dim_new['date_time']= date_dim_new['date_time']
    date_dim_new['date'] = date_dim_new['date_time'].dt.date
    date_dim_new['year'] = date_dim_new['date_time'].dt.year
    date_dim_new['month'] = date_dim_new['date_time'].dt.month
    date_dim_new['day'] = date_dim_new['date_time'].dt.day
    date_dim_new['weekday'] = date_dim_new['date_time'].dt.day_name()
    date_dim_new['hour'] = date_dim_new['date_time'].dt.hour
    date_dim_new['is_weekend'] = date_dim_new['weekday'].isin(['Saturday', 'Sunday'])

    if os.path.exists(DATE_DIM_FILE):
        date_dim_existing = pd.read_csv(DATE_DIM_FILE)
        date_dim_existing['date_time'] = pd.to_datetime(date_dim_existing['date_time'])
        date_dim = pd.concat([date_dim_existing, date_dim_new]).drop_duplicates(subset=['date_time']).reset_index(drop=True)
    else:
        date_dim = date_dim_new

    date_dim['date_sk'] = range(len(date_dim))
    date_dim = date_dim[['date_sk'] + [c for c in date_dim.columns if c != 'date_sk']]
    date_dim.to_csv(DATE_DIM_FILE, index=False)



    
    weather_dim_new = df_merged[['weather_id', 'weather_main', 'weather_description', 'weather_icon']].drop_duplicates().reset_index(drop=True)
    if os.path.exists(WEATHER_DIM_FILE):
        weather_dim_existing = pd.read_csv(WEATHER_DIM_FILE)
        weather_dim = pd.concat([weather_dim_existing, weather_dim_new]).drop_duplicates(subset=['weather_id']).reset_index(drop=True)
    else:
        weather_dim = weather_dim_new
    weather_dim['weather_sk'] = range(len(weather_dim))
    weather_dim = pd.concat([weather_dim[['weather_sk']], weather_dim.drop(columns=['weather_sk'], errors='ignore')], axis=1)
    weather_dim.to_csv(WEATHER_DIM_FILE, index=False)



    location_dim_new = df_merged.reset_index()[['city', 'sys_country', 'coord_lon', 'coord_lat', 'timezone','base']].drop_duplicates().reset_index(drop=True)
    if os.path.exists(LOCATION_DIM_FILE):
        location_dim_existing = pd.read_csv(LOCATION_DIM_FILE)
        location_dim = pd.concat([location_dim_existing, location_dim_new]).drop_duplicates(subset=['city']).reset_index(drop=True)
    else:
        location_dim = location_dim_new
    location_dim['location_sk'] = range(len(location_dim))
    location_dim = pd.concat([location_dim[['location_sk']], location_dim.drop(columns=['location_sk'], errors='ignore')], axis=1)
    location_dim.to_csv(LOCATION_DIM_FILE, index=False)



    pollutants_fact = pd.concat([
    df_merged.filter(regex='^pollution'),
    df_merged[['weather_id', 'city']]
    ], axis = 1).drop_duplicates().reset_index(drop=True)
    
    pollutants_fact['pollution_dt'] = pollutants_fact['pollution_dt'].dt.floor('H')

    pollutants_fact = pollutants_fact.merge(
    date_dim[['date_sk', 'date_time']],
    left_on= 'pollution_dt',
    right_on='date_time',
    how='inner'
    ).merge(
    location_dim[['city', 'location_sk']],
    on='city',
    how='left',
    ).merge(
    weather_dim[['weather_id', 'weather_sk']],
    on='weather_id',
    how='left'
    ).drop(columns=['pollution_dt', 'weather_id', 'date_time', 'city'])

    if os.path.exists(POLLUTANTS_FACT_FILE):
        pollutants_fact_existing = pd.read_csv(POLLUTANTS_FACT_FILE)
        pollutants_fact = pd.concat([pollutants_fact_existing, pollutants_fact]).drop_duplicates().reset_index(drop=True)
    else:
        pollutants_fact = pollutants_fact

    pollutants_fact['pollutants_sk'] = range(len(pollutants_fact))
    cols = ['pollutants_sk', 'location_sk', 'weather_sk','date_sk']+[c for c in pollutants_fact.columns if c not in ['pollutants_sk', 'date_sk', 'location_sk', 'weather_sk']]
    pollutants_fact = pollutants_fact[cols]

    pollutants_fact.to_csv(POLLUTANTS_FACT_FILE, index=False)



    weather_fact = pd.concat([df_merged.filter(regex='^main'), df_merged[['sys_sunrise','sys_sunset','visibility','dt','weather_id','city']]], axis=1).drop_duplicates().reset_index(drop=True)
    weather_fact['sys_sunrise'] = weather_fact['sys_sunrise'].dt.floor('H')
    weather_fact['sys_sunset'] = weather_fact['sys_sunset'].dt.floor('H')
    weather_fact['dt'] = weather_fact['dt'].dt.floor('H')
    weather_fact = weather_fact.merge(date_dim[['date_sk','date_time']], left_on='sys_sunrise', right_on='date_time', how='left').rename(columns={'date_sk':'sunrise_date_sk'}).drop(columns=['date_time','sys_sunrise'])
    weather_fact = weather_fact.merge(date_dim[['date_sk','date_time']], left_on='sys_sunset', right_on='date_time', how='left').rename(columns={'date_sk':'sunset_date_sk'}).drop(columns=['date_time','sys_sunset'])
    weather_fact = weather_fact.merge(date_dim[['date_sk','date_time']], left_on='dt', right_on='date_time', how='left').rename(columns={'date_sk':'date_sk'}).drop(columns=['date_time','dt'])
    weather_fact = weather_fact.merge(location_dim[['city','location_sk']], on='city', how='left').drop(columns=['city'])
    weather_fact = weather_fact.merge(weather_dim[['weather_id','weather_sk']], on='weather_id', how='left').drop(columns=['weather_id'])

    if os.path.exists(WEATHER_FACT_FILE):
        weather_fact_existing = pd.read_csv(WEATHER_FACT_FILE)
        weather_fact = pd.concat([weather_fact_existing, weather_fact]).drop_duplicates().reset_index(drop=True)
    weather_fact['weather_fact_sk'] = range(len(weather_fact))
    cols = ['weather_fact_sk','weather_sk','location_sk','date_sk','sunrise_date_sk','sunset_date_sk'] + [c for c in weather_fact.columns if c not in ['weather_fact_sk','weather_sk','location_sk','date_sk','sunrise_date_sk','sunset_date_sk']]
    weather_fact = weather_fact[cols]
    weather_fact.to_csv(WEATHER_FACT_FILE, index=False)

data_cleaning()
