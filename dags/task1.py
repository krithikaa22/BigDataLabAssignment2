from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import pandas as pd
import os
import random
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
from airflow.models import Variable
import apache_beam as beam


beam_pipeline_py_file = Variable.get('beam')


def get_data(**context):
    year = 2023
    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find("table").find_all("tr")[2:-2]

    fileName = []

    total = 10

    for i in range(total):
        index = random.randint(0, len(rows))
        data = rows[index].find_all("td")
        fileName.append(data[0].text)
    
    for name in fileName:
        newUrl = url+name
        response = requests.get(newUrl)
        open(name,'wb').write(response.content)

    with ZipFile('weather.zip','w') as zip:
        for file in fileName:
            zip.write(file)
    os.system('mv weather.zip /opt/airflow/dags/')
    print(os.path.isdir('/weather.zip'))

def unzip(**context):
   with ZipFile("/opt/airflow/dags/weather.zip", 'r') as zObject: 
    zObject.extractall(path="./") 

   print(os.list('/opt/airflow/dags'))

# p = beam.Pipeline()

# def run_apache_beam_pipeline(input_csv_path):
#         result_tuple = (
#                 p
#                 | 'Read CSV File' >> beam.io.ReadFromText(input_csv_path, skip_header_lines=1)
#                 | 'Extract and Filter' >> beam.ParDo(ExtractAndFilterDoFn())
#             )

#         return result_tuple

# class ExtractAndFilterDoFn(beam.DoFn):
#     def process(self, element):
#         lat = element['LATITUDE'][0]
#         lon = element['LONGITUDE'][0]
#         bulbtemp = element['HourlyWetBulbTemperature']
#         windspeed = element['HourlyWindSpeed']
#         month = pd.to_datetime(element['DATE']).dt.month

#         return (lat, lon, [[windspeed, bulbtemp]], month)

def process_data_file(**context):
        files = os.listdir('./')
        files = [file for file in files if file.endswith('.csv')]
        tuple_list = []
        for f in files:
            tuple_list.extend(beam_pipeline_py_file(f))
        print("Processed Result:", tuple_list)

# def filter_data(**context):
#     files = os.listdir('./')
#     files = [file for file in files if file.endswith('.csv')]

#     tuple_list = []
#     for f in files:
#         df = pd.read_csv(f)
#         bulbtemp = df['HourlyWetBulbTemperature']
#         windspeed = df['HourlyWindSpeed']
#         month = pd.to_datetime(df['DATE']).dt.month
#         lattitude = df['LATITUDE'][0]
#         longitude = df['LONGITUDE'][0]
#         tuple_list.append((lattitude, longitude, bulbtemp.tolist(), windspeed.tolist(), month.tolist()))


dag_1 = DAG(
    dag_id= "fetch_data",
    schedule_interval="@daily",
    default_args={
            "owner": "first_task",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
    catchup=False
)

dag_2 = DAG(
    dag_id= "data_analysis",
    schedule_interval="@daily",
    default_args={
            "owner": "first_task",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
    catchup=False
)
    
get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        provide_context=True,
        op_kwargs={"name":"first_task"},
        dag=dag_1
    )

unzip_data = PythonOperator(
        task_id="wait_data",
        python_callable=unzip,
        provide_context=True,
        dag=dag_1
    )

    # file_sensor_task = FileSensor(
    # task_id='file_sensor_task',
    # filepath='/opt/airflow/dags/weather.zip',
    # conn_id='fs_default',
    # timeout=5,  # Timeout in seconds
    # poke_interval=1,  # Check file existence every 1 second
    # mode='poke',  # Use 'poke' mode for actively polling the file
    # retries=3,  # Number of retries before giving up
    # )

filter_data = PythonOperator(
    task_id='filter_data',
    python_callable=process_data_file,
    )


get_data >> unzip_data >> filter_data