🌤️ pollution_fetch_api
Overview

pollution_fetch_api is an end-to-end data pipeline that automates the collection, cleaning, and loading of weather and air pollution data from the OpenWeather API into a Snowflake data warehouse.
The pipeline is fully orchestrated with Apache Airflow and containerized using Docker, ensuring scalability, reliability, and repeatability for long-term environmental data analysis.

🚀 Tech Stack

Python – Data processing and transformation (Pandas, JSON handling)

Apache Airflow – Workflow orchestration and task scheduling

Docker – Containerized environment for Airflow and dependencies

Snowflake – Cloud data warehouse for structured data storage

Pandas – Data cleaning, transformation, and feature extraction

OpenWeather API – Data source for weather and air pollution metrics

🌍 Data Source

API: OpenWeather API
Endpoints Used:
Current Weather Data
Air Pollution Data

Each DAG run fetches weather and pollution data for a set of selected cities, ensuring consistent updates and enabling time-series analysis of environmental metrics.

🧩 Pipeline Overview

Fetch Data
Airflow triggers API calls to OpenWeather endpoints to collect weather and pollution data in JSON format.

Transform & Clean
The data is normalized and cleaned using Pandas:

Flatten nested JSON structures

Create dimension tables (Date, Location, Weather)

Build fact tables (Pollutants Fact, Weather Fact)

Load to Snowflake
Cleaned data is written to CSV files, staged, and loaded into Snowflake using the COPY INTO command for optimized ingestion.

Schedule & Automation
Airflow schedules the DAG to run automatically (daily or hourly), ensuring continuous data growth for historical and trend analysis.

🏗️ Data Warehouse Schema

Dimensions

date_dim: stores timestamps and calendar attributes

location_dim: stores city-specific metadata

weather_dim: stores unique weather condition records

Facts

pollutants_fact: stores pollutant concentration metrics with foreign keys to dimensions

weather_fact: stores main weather metrics (temperature, pressure, humidity, etc.)

⚙️ Project Structure
pollution_fetch_api/
│
├── dags/
│   ├── fetchin_cleaning.py        # Main Airflow DAG
│   ├── cleaning_data.py           # Data transformation logic
│
├── docker-compose.yml             # Docker setup for Airflow
├── requirements.txt               # Python dependencies
├── README.md                      # Project documentation
└── data/
    ├── weather_data.json
    ├── pollution_data.json
    └── processed CSVs (dimensions & facts)

🧠 Key Highlights

Designed using modular ETL principles

Ensures data integrity via dimension and fact table design

Uses incremental updates (appends new records without overwriting old ones)

Fully automated pipeline — from API fetch to Snowflake loading

Easily scalable for new data sources or additional cities

💾 Setup & Usage

1. Clone the Repository
git clone https://github.com/<your-username>/pollution_fetch_api.git
cd pollution_fetch_api

2. Start Docker Containers
docker-compose up -d

3. Access Airflow UI
URL: http://localhost:8080
Default credentials: airflow / airflow

4. Trigger DAG
Run the DAG named weather_to_snowflake_pipeline

🧑‍💻 Author

Karim Baraka
📧 https://www.linkedin.com/in/karim-yasser-372874319/
💼 Data Engineer | Python | Airflow | Snowflake | SQL
