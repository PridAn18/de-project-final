from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable

import pendulum
import vertica_python

import json

conn_info_json = Variable.get("vertica_conn")
conn_info = json.loads(conn_info_json)

def global_metrics_VIEW():
    try:
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE OR REPLACE VIEW STV2024101038__DWH.global_metrics AS
                    WITH RatesCurrency AS (        
                    SELECT c.currency_code, c.currency_code_with, c.currency_with_div
                        FROM STV2024101038__STAGING.currencies c
                        
                        WHERE c.date_update = (SELECT MAX(date_update) FROM STV2024101038__STAGING.currencies) and c.currency_code_with = (SELECT c.currency_code
                        FROM STV2024101038__STAGING.currencies c
                        LEFT JOIN
                            STV2024101038__STAGING.transactions t ON t.currency_code = c.currency_code
                        WHERE t.country = 'usa'
                        LIMIT 1 )
                        
                    ),
                    TransactionData AS (
                        SELECT
                                t.operation_id, 
                                t.account_number_from,
                                t.account_number_to,
                                t.currency_code,
                                t.amount,
                                t.transaction_dt
                            FROM
                                STV2024101038__STAGING.transactions t
                            where t.account_number_from <> -1
                            
                    )
                    SELECT
                        td.transaction_dt::date AS date_update,
                        td.currency_code AS currency_from,
                        SUM(td.amount * COALESCE(rc.currency_with_div, 1)) AS amount_total,  -- Перевод суммы в доллары
                        COUNT(td.operation_id) AS cnt_transactions,  -- Общее количество транзакций
                        AVG(td.amount) AS avg_transactions_per_account,  -- Средний объем транзакций с аккаунта
                        COUNT(DISTINCT td.account_number_from ) AS cnt_accounts_make_transactions  -- Количество уникальных аккаунтов
                    FROM
                        TransactionData td
                        LEFT JOIN
                            RatesCurrency rc ON td.currency_code = rc.currency_code
                            
                    GROUP BY
                        td.transaction_dt::date,
                        td.currency_code
                    ORDER BY
                        date_update,
                        currency_from;
                        """)
                print(f"Данные успешно загружены в витрины global_metrics.")
    
    except Exception as e:
        print(f"Произошла ошибка при создании витрины данных: {e}.")

@dag(schedule_interval='0 0 * * *', start_date=pendulum.parse('2022-10-01')) # @daily
def datamart_dag():
    
    

    global_metrics_VIEW_task = PythonOperator(
        task_id='global_metrics_VIEW',
        python_callable=global_metrics_VIEW
    )


    global_metrics_VIEW_task


_ = datamart_dag()

