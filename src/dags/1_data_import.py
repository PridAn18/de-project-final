from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable

import pendulum
import boto3
import vertica_python

import os

import json

AWS_ACCESS_KEY_ID = Variable.get("aws_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_key") # переменные в airflow variables

conn_info_json = Variable.get("vertica_conn")
conn_info = json.loads(conn_info_json)

def fetch_s3_file(key: str): #создаём клиент S3 в таске, так как в конфиге Airflow отключён xcom_pickling
    s3_client = boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    # Создаем директорию data
    os.makedirs('./data', exist_ok=True)
    try:
        s3_client.download_file(
            Bucket='final-project',
            Key=key,
            Filename=f'./data/{key}'  
        )
        print(f"Файл {key} успешно загружен.")
    except Exception as e:
        print(f"Ошибка при загрузке файла {key}: {e}")

def load_s3_file(dataset: str, tablename: str):
    """Загружает данные в Vertica, используя COPY."""
    
    try:
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT column_name 
                FROM v_catalog.columns 
                WHERE table_name = '{tablename}';
                """)
                columns = [row[0] for row in cur.fetchall()]
                if tablename == 'currencies':
                    columns_list = 'currency_code, currency_code_with, date_update, currency_with_div'
                else:
                    columns_list = ', '.join(columns)
                # Выполняем команду COPY
                cur.execute(f"""
                COPY STV2024101038__STAGING.{tablename} ({columns_list})
                FROM LOCAL './data/{dataset}.csv'
                DELIMITER ',';
                """)
                print(f"Данные из {dataset}.csv успешно загружены в таблицу {tablename}.")
    
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных из {dataset}: {e}")

def h_currency_insert():
    try:
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                INSERT INTO STV2024101038__DWH.h_currency ( h_currency_pk, currency_code, load_dt, load_src )
                SELECT DISTINCT
                    hash(c.currency_code) AS h_currency_pk,
                    c.currency_code,
                    now() AS load_dt,
                    's3' AS load_src
                FROM STV2024101038__STAGING.currencies c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM STV2024101038__DWH.h_currency h
                    WHERE h.h_currency_pk = hash(c.currency_code)
                )
                            
                UNION

                SELECT DISTINCT
                    hash(c.currency_code_with) AS h_currency_pk,
                    c.currency_code_with,
                    now() AS load_dt,
                    's3' AS load_src
                FROM STV2024101038__STAGING.currencies c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM STV2024101038__DWH.h_currency h
                    WHERE h.h_currency_pk = hash(c.currency_code_with)
                );
                """)
                print(f"Данные успешно загружены в таблицу h_currency.")
    
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных в таблицу h_currency: {e}.")

def s_currency_history_insert():
    try:
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                INSERT INTO STV2024101038__DWH.s_currency_history ( hk_currency_hashdiff, h_currency_pk, h_currency_with_pk, currency_code_div, date_update, load_dt, load_src )
                SELECT DISTINCT
                    hash(c.currency_code, c.currency_code_with, c.date_update) AS hk_currency_hashdiff,
                    hc.h_currency_pk,
                    hc1.h_currency_pk,
                    c.currency_with_div,
                    c.date_update,
                    now() AS load_dt,
                    's3' AS load_src
                FROM STV2024101038__STAGING.currencies c
                left join STV2024101038__DWH.h_currency as hc on c.currency_code = hc.currency_code
                left join STV2024101038__DWH.h_currency as hc1 on c.currency_code_with = hc1.currency_code
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM STV2024101038__DWH.s_currency_history h
                    WHERE h.hk_currency_hashdiff = hash(c.currency_code, c.currency_code_with, c.date_update)
                );
                """)
                print(f"Данные успешно загружены в таблицу s_currency_history.")
    
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных в таблицу s_currency_history: {e}.")

def h_accounts_insert():
    try:
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                INSERT INTO STV2024101038__DWH.h_accounts ( h_account_pk, account_id, load_dt, load_src )
                SELECT DISTINCT
                    hash(t.account_number_from) AS h_account_pk,
                    t.account_number_from as account_id,
                    now() AS load_dt,
                    's3' AS load_src
                FROM STV2024101038__STAGING.transactions t
                WHERE t.account_number_from <> '-1' 
                AND NOT EXISTS (
                    SELECT 1
                    FROM STV2024101038__DWH.h_accounts h
                    WHERE h.h_account_pk = hash(t.account_number_from)
                    
                )
                UNION
                SELECT DISTINCT
                    hash(t.account_number_to) AS h_account_pk,
                    t.account_number_to as account_id,
                    now() AS load_dt,
                    's3' AS load_src
                FROM STV2024101038__STAGING.transactions t
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM STV2024101038__DWH.h_accounts h
                    WHERE h.h_account_pk = hash(t.account_number_to)
                );
                """)
                print(f"Данные успешно загружены в таблицу h_accounts.")
    
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных в таблицу h_accounts: {e}.")

def l_transactions_insert():
    try:
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                INSERT INTO STV2024101038__DWH.l_transactions ( operation_id, h_account_from_pk, h_account_to_pk, h_currency_pk, load_dt, load_src )
                SELECT DISTINCT
                    t.operation_id,
                    ha.h_account_pk,
                    ha1.h_account_pk,
                    hc.h_currency_pk,
                    now() AS load_dt,
                    's3' AS load_src
                FROM STV2024101038__STAGING.transactions t
                left join STV2024101038__DWH.h_currency as hc on t.currency_code = hc.currency_code
                left join STV2024101038__DWH.h_accounts as ha on t.account_number_from = ha.account_id
                left join STV2024101038__DWH.h_accounts as ha1 on t.account_number_to = ha1.account_id
                WHERE ha.h_account_pk is not null and NOT EXISTS (
                    SELECT 1
                    FROM STV2024101038__DWH.l_transactions h
                    WHERE h.operation_id = t.operation_id
                );
                """)
                print(f"Данные успешно загружены в таблицу l_transactions.")
    
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных в таблицу l_transactions: {e}.")

def s_transaction_details_insert():
    try:
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                INSERT INTO STV2024101038__DWH.s_transaction_details ( transaction_type, amount,
                country, status, transaction_dt, load_dt, load_src, hk_transaction_hashdiff,
                operation_id )
                SELECT DISTINCT
                    t.transaction_type,
                    t.amount,
                    t.country,
                    t.status,
                    t.transaction_dt, 
                    now() AS load_dt,
                    's3' AS load_src,
                    hash(t.operation_id, t.status) AS hk_transaction_hashdiff,
                    t.operation_id
                FROM STV2024101038__STAGING.transactions t
                WHERE t.account_number_from <> '-1' and NOT EXISTS (
                    SELECT 1
                    FROM STV2024101038__DWH.s_transaction_details h
                    WHERE h.hk_transaction_hashdiff = hash(t.operation_id, t.status)
                );
                """)
                print(f"Данные успешно загружены в таблицу s_transaction_details.")
    
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных в таблицу s_transaction_details: {e}.")


@dag(schedule_interval=None, start_date=pendulum.parse('2022-10-01')) # @daily
def s3_dag():
    
    
    bucket_files = ['currencies_history']
    for i in range(1, 11):
        bucket_files.append(f'transactions_batch_{i}')
    bucket_transactions = []
    for i in range(1, 11):
        bucket_transactions.append(f'transactions_batch_{i}')

    

    fetch_s3_file_tasks = []
    for f in bucket_files:
        fetch_s3_file_task = PythonOperator(
            task_id=f'fetch_s3_file_{f}',
            python_callable=fetch_s3_file,
            op_kwargs={
                'key': f'{f}.csv'
            }
        )
        fetch_s3_file_tasks.append(fetch_s3_file_task)


    load_s3_currencies_history_file_task = PythonOperator(
        task_id='load_currencies_history_s3_file',
        python_callable=load_s3_file,
        op_kwargs={'dataset': 'currencies_history', 'tablename': 'currencies'}
    )

    load_s3_transactions_file_tasks = []
    for f in bucket_transactions:
        load_s3_transactions_file_task = PythonOperator(
            task_id=f'load_transactions_s3_file_{f}',  # Уникальный ID для каждой задачи
            python_callable=load_s3_file,
            op_kwargs={
                'dataset': f,  # Передаем только один файл
                'tablename': 'transactions'
            }
        )
        load_s3_transactions_file_tasks.append(load_s3_transactions_file_task)

    h_currency_insert_task = PythonOperator(
        task_id='h_currency_insert',
        python_callable=h_currency_insert
    )

    s_currency_history_insert_task = PythonOperator(
        task_id='s_currency_history_insert',
        python_callable=s_currency_history_insert
    )

    h_accounts_insert_task = PythonOperator(
        task_id='h_accounts_insert',
        python_callable=h_accounts_insert
    )

    l_transactions_insert_task = PythonOperator(
        task_id='l_transactions_insert',
        python_callable=l_transactions_insert
    )

    s_transaction_details_insert_task = PythonOperator(
        task_id='s_transaction_details_insert',
        python_callable=s_transaction_details_insert
    )

    # Устанавливаем зависимости

    fetch_s3_file_tasks >> load_s3_currencies_history_file_task
    
    load_s3_currencies_history_file_task >> load_s3_transactions_file_tasks

    load_s3_transactions_file_tasks >> h_currency_insert_task >> s_currency_history_insert_task >> h_accounts_insert_task >> l_transactions_insert_task >> s_transaction_details_insert_task

    s_transaction_details_insert_task


_ = s3_dag()

