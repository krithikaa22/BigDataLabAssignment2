from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import os
import random
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
import apache_beam as beam
from airflow.models import Variable
import numpy as np
import pandas as pd
import urllib
import shutil
from datetime import datetime, timedelta
import geopandas as gpd
from geodatasets import get_path
import logging
from ast import literal_eval as make_tuple
import matplotlib.pyplot as plt

## Function to get data
def get_data(**context):
    year = 2023
    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/'

    ## Request to the API

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find("table").find_all("tr")[2:-2]

    fileName = []

    total = 10

    for i in range(total):
        index = random.randint(0, len(rows))
        data = rows[index].find_all("td")
        fileName.append(data[0].text)
    
    ## Writing binary data onto the local directory

    for name in fileName:
        newUrl = url+name
        response = requests.get(newUrl)
        open(name,'wb').write(response.content)

    ## Zipping the files

    with ZipFile('/root/airflow/DAGS/weather.zip','w') as zip:
        for file in fileName:
            zip.write(file)


## Function to unzip the zipped files
def unzip(**context):
   with ZipFile("/root/airflow/DAGS/weather.zip", 'r') as zObject: 
      zObject.extractall(path="/root/airflow/DAGS/files") 

## DAG for task 1
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

get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        provide_context=True,
        op_kwargs={"name":"first_task"},
        dag=dag_1
    )


get_data


## Function to parse csv
def parseCSV(data):
    df = data.split('","')
    df[0] = df[0].strip('"')
    df[-1] = df[-1].strip('"')
    return list(df)

class ExtractFields(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)
        self.headers = {i:ind for ind,i in enumerate(headers)} 


    ## Function to get the required fields from the csv
    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            yield ((lat, lon), data)


## Function to run beam for getting fields from csv
def filter_data(**kwargs):
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    os.makedirs('/root/airflow/DAGS/files/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/root/airflow/DAGS/files/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTuple' >> beam.ParDo(ExtractFields(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/result.txt')


class MonthlyData(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)

        self.headers = {i:ind for ind,i in enumerate(headers)}

    ## Function to get monthly data
    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            time = datetime.strptime(element[headers['DATE']],'%Y-%m-%dT%H:%M:%S')
            Format = "%Y-%m"
            month = time.strftime(Format)
            yield ((month, lat, lon), data)


## Function to calculate averages
def compute_avg(data):
    val = np.array(data[1])
    shape = val.shape
    val = pd.to_numeric(val.flatten(), errors='coerce',downcast='float') 
    val = np.reshape(val, shape)
    masked = np.ma.masked_array(val, np.isnan(val))
    res = np.ma.average(masked, axis=0)
    res = list(res.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(res)
    return ((data[0][1],data[0][2]),res)


## Function to use beam to compute the averages
def compute_monthly_avg( **kwargs):
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTuple' >> beam.ParDo(MonthlyData(required_fields=required_fields))
            | 'MonthlyTuple' >> beam.GroupByKey()
            | 'Averages' >> beam.Map(lambda data: compute_avg(data))
            | 'AveragesTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/results/averages.txt')
        

class Aggregated(beam.CombineFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i in headers_csv:
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(i.replace('Hourly',''))

    def create_accumulator(self):
        return []
    
    def add_input(self, accumulator, element):
        accumulator_2 = {key:value for key,value in accumulator}
        data = element[2]
        val = np.array(data)
        shape = val.shape
        val = pd.to_numeric(val.flatten(), errors='coerce',downcast='float')
        val = np.reshape(val,shape)
        masked = np.ma.masked_array(val, np.isnan(val))
        res = np.ma.average(masked, axis=0)
        res = list(res.filled(np.nan))
        for ind,i in enumerate(self.required_fields):
            accumulator_2[i] = accumulator_2.get(i,[]) + [(element[0],element[1],res[ind])]

        return list(accumulator_2.items())
    
    def merge_accumulators(self, accumulators):
        merged = {}
        for a in accumulators:
                a2 = {key:value for key,value in a}
                for i in self.required_fields:
                    merged[i] = merged.get(i,[]) + a2.get(i,[])

        return list(merged.items())
    
    def extract_output(self, accumulator):
        return accumulator
    

## Plot geomaps
def plot_geomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    data = np.array(values[1],dtype='float')
    d = np.array(data,dtype='float')


    data = gpd.GeoDataFrame({
        values[0]:d[:,2]
    }, geometry=gpd.points_from_xy(*d[:,(1,0)].T))
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)

    ax.set_title(f'{values[0]} Heatmap')

    os.makedirs('/root/airflow/DAGS/results/plots', exist_ok=True)
    
    plt.savefig(f'/root/airflow/DAGS/results/plots{values[0]}_heatmap_plot.png')


## Function to use beam to create plots
def creae_heatmap(**kwargs):
    
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    with beam.Pipeline(runner='DirectRunner') as p:
        
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/results/averages.txt*')
            | 'Parse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(Aggregated(required_fields = required_fields))
            | 'Flatten' >> beam.FlatMap(lambda a:a) 
            | 'Geomaps' >> beam.Map(plot_geomaps)            
        )
        

## Function to delete csv
def delete_csv(**kwargs):
    shutil.rmtree('/root/airflow/DAGS/files')

## DAG for task 2
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


wait = FileSensor(
    task_id = 'wait',
    mode="poke",
    poke_interval = 5,  
    timeout = 5,  
    filepath = "/root/airflow/DAGS/weather.zip",
    dag=dag_2,
    fs_conn_id = "fs_default", 
)

unzip_task = PythonOperator(
        task_id="unzip_task",
        python_callable=unzip,
        provide_context=True,
        dag=dag_2
    )

filter_data_task = PythonOperator(
    task_id='filter_data',
    python_callable=filter_data,
    dag=dag_2,
)

avg = PythonOperator(
    task_id='monthy_averages',
    python_callable=compute_monthly_avg,
    dag=dag_2,
)

create_heatmap = PythonOperator(
    task_id='heatmap',
    python_callable=creae_heatmap,
    dag=dag_2,
)

delete_csv = PythonOperator(
    task_id='delete',
    python_callable=delete_csv,
    dag=dag_2,
)

wait >> unzip_task >> filter_data_task >> avg >> create_heatmap >> delete_csv