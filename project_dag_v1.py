import pandas as pd
import requests
import xml.etree.ElementTree as ET
import bs4
from bs4 import BeautifulSoup
import pendulum
from pyspark.sql.functions import replace
from pyspark.sql import SparkSession
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id = 'aqi_dag_v1',
    start_date = pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup = False,
    schedule_interval='@hourly'
) as dag:

    @task(task_id = 'parsing_data')
    def get_aqi(**kwargs):
        url = 'https://www.iqair.com/russia/moscow' # Москва
        url1 = 'https://www.iqair.com/ru/russia/kaliningrad' # Калиниград
        url2 = 'https://www.iqair.com/russia/st-petersburg/saint-petersburg' # Санкт-Петербург
        url3 = 'https://www.iqair.com/russia/novosibirsk' # Новосибирск
        url4 = 'https://www.iqair.com/russia/krasnoyarsk-krai/krasnoyarsk' # Красноярск
        url5 = 'https://www.iqair.com/russia/chelyabinsk' # Челябинск
        url6 = 'https://www.iqair.com/russia/krasnodarskiy/krasnodar' # Краснодар

        response = requests.get(url)
        print(response)
        response1 = requests.get(url1)
        print(response1)
        response2 = requests.get(url2)
        print(response2)
        response3 = requests.get(url3)
        print(response3)
        response4 = requests.get(url4)
        print(response4)
        response5 = requests.get(url5)
        print(response5)
        response6 = requests.get(url6)
        print(response6)


        bs = BeautifulSoup(response.text)
        temp = bs.find('p', 'aqi-value__value')
        aqi_moscow = temp.text

        bs1 = BeautifulSoup(response1.text)
        temp1 = bs1.find('p', 'aqi-value__value')
        aqi_kaliningrad = temp1.text

        bs2 = BeautifulSoup(response2.text)
        temp2 = bs2.find('p', 'aqi-value__value')
        aqi_2 = temp2.text

        bs3 = BeautifulSoup(response3.text)
        temp3 = bs3.find('p', 'aqi-value__value')
        aqi_3 = temp3.text

        bs4 = BeautifulSoup(response4.text)
        temp4 = bs4.find('p', 'aqi-value__value')
        aqi_4 = temp4.text

        bs5 = BeautifulSoup(response5.text)
        temp5 = bs5.find('p', 'aqi-value__value')
        aqi_5 = temp5.text

        bs6 = BeautifulSoup(response6.text)
        temp6 = bs6.find('p', 'aqi-value__value')
        aqi_6 = temp6.text

        results = {'City': ['Moscow','Kaliningrad', 'Sankt_petersburg', 'Novosibirsk', 'Krasnoyarsk', 'Cheslyabinsk', 'Krasnodar'], 'AQI': [aqi_moscow, aqi_kaliningrad, aqi_2, aqi_3, aqi_4, aqi_5, aqi_6] }
        df = pd.DataFrame(results)

       	spark = SparkSession.builder.master("local").appName('aqi_cities').getOrCreate()

        aqi_results = spark.createDataFrame(df)

        aqi_results.repartition(1).write.mode('overwrite').parquet('/user/george_cheshko_project/project/temp')

    @task(task_id = 'output_data')
    def send_aqi(**kwargs):
        spark = SparkSession.builder.master("local").appName('aqi_cities').config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar").getOrCreate()
        df = spark.read.parquet('/user/george_cheshko_project/project/temp')
        print(df)
        pandas_df = df.toPandas()
        TOKEN = "6957938390:AAHoJUPX4siUlLm1lDigQ8KopIcEvsPiIVQ"
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        data = requests.get(url).json()
        # Извлекаем ID из словаря
        id = [data['result'][0]['message']['from']['id']]

        for i in id:
          TOKEN = "6957938390:AAHoJUPX4siUlLm1lDigQ8KopIcEvsPiIVQ"
          chat_id = i
          message = f"Текущий индекс качества воздуха"
          message1 = f"{pandas_df}"
          url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
          url1 = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message1}"
          print(requests.get(url).json())
          print(requests.get(url1).json())


    get_aqi()>>send_aqi()