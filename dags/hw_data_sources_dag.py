import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Функция обработки JSON
def flatten_json():
    # сырой джсон
    json_url = 'https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json'
    
    # Загружаем данные
    response = requests.get(json_url)
    data = response.json()
    
    # 1. Разворачиваем основной уровень 
    df = pd.json_normalize(data['pets'])
    
    
    df_flat = df.explode('favFoods').reset_index(drop=True)
    output_path = '/opt/airflow/dags/pets_flattened.csv'
    df_flat.to_csv(output_path, index=False)
    print(f"JSON успешно преобразован в плоскую структуру: {output_path}")

# 2. Функция обработки XML
def flatten_xml():
    # сырой xml
    xml_url = 'https://gist.githubusercontent.com/pamelafox/3000322/raw/4bd276bf656b26ddf647c2115ec661bc1d3d63b0/nutrition.xml'
    
    response = requests.get(xml_url)
    root = ET.fromstring(response.content)
    
    rows = []
    
    # Итерируемся только по тегам <food>, игнорируя <daily-values>
    for food in root.findall('food'):
        row = {}
        
      
        for child in food:
            if len(child) == 0 and not child.attrib:
                row[child.tag] = child.text
                
        # 2. Обработка тегов с атрибутами (serving, calories)
        serving = food.find('serving')
        if serving is not None:
            row['serving_units'] = serving.attrib.get('units', '')
            row['serving_amount'] = serving.text
            
        calories = food.find('calories')
        if calories is not None:
            row['calories_total'] = calories.attrib.get('total', '0')
            row['calories_fat'] = calories.attrib.get('fat', '0')
            
        # 3. Обработка вложенных тегов (vitamins)
        vitamins = food.find('vitamins')
        if vitamins is not None:
            for v in vitamins:
                row[f'vitamin_{v.tag}'] = v.text
                
        # 4. Обработка вложенных тегов (minerals)
        minerals = food.find('minerals')
        if minerals is not None:
            for m in minerals:
                row[f'mineral_{m.tag}'] = m.text
                
        rows.append(row)
        
    df = pd.DataFrame(rows)
    output_path = '/opt/airflow/dags/nutrition_flattened.csv'
    df.to_csv(output_path, index=False)
    print(f"XML успешно преобразован в плоскую структуру: {output_path}")

# Настройки дага
default_args = {
    'owner': 'Daniil Tyurin',
    'retries': 1,
}

with DAG(
    dag_id='tyurin_daniil_hw_data_sources', 
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['hw', 'ozerkov', 'etl', 'sources']
) as dag:

    task_json = PythonOperator(
        task_id='process_json_source',
        python_callable=flatten_json
    )

    task_xml = PythonOperator(
        task_id='process_xml_source',
        python_callable=flatten_xml
    )

    # Запускаем 
    [task_json, task_xml]
