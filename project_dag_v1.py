import pandas as pd
import requests
import xml.etree.ElementTree as ET
import bs4

from pyspark.sql.functions import replace 
from pyspark.sql import SparkSession

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id = 'aqi_dag_v1',
    start_date = pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup = False
) as dag:

    @task(task_id = 'parsing_data')
    def get_aqi(**kwargs):
        #current_date = kwargs['ds']

        #cbr_date = datetime.strptime(current_date, '%Y-%m-%d').strftime("%d/%m/%Y")

        url = 'https://www.iqair.com/russia/moscow'
        url1 = 'https://www.iqair.com/ru/russia/kaliningrad'
        
        response = requests.get(url)
        print(response)
        response1 = requests.get(url1)
        print(response1)


        bs = BeautifulSoup(response.text,"lxml")
        temp = bs.find('p', 'aqi-value__value')
        aqi_moscow = temp.text

        bs1 = BeautifulSoup(response1.text,"lxml")
        temp1 = bs1.find('p', 'aqi-value__value')
        aqi_kaliningrad = temp1.text

        results = {'City': ['Moscow','Kaliningrad'], 'AQI': [aqi_moscow, aqi_kaliningrad] }
        df = pd.DataFrame(results)

        spark = SparkSession.builder.master("local").appName('aqi_cities').getOrCreate()

        aqi_results = spark.createDataFrame(df)

        aqi_results.repartition(1).write.mode('overwrite').parquet('/user/george_cheshko/project/temp')
    get_aqi() 