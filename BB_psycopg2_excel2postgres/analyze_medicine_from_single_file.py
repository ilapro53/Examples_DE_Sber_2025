import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv('.env')

try:
    print('Создание подключения к PostgreSQL...')

    # Создание подключения к PostgreSQL
    conn = psycopg2.connect(
        database = os.getenv('POSTGRES_DB_NAME'),
        host = os.getenv('POSTGRES_DB_HOST'),
        user = os.getenv('POSTGRES_DB_USER'),
        password = os.getenv('POSTGRES_DB_PASSWORD'),
        port = os.getenv('POSTGRES_DB_PORT'),
    )

    # Отключение автокоммита
    conn.autocommit = False

    # Создание курсора
    cursor = conn.cursor()

    ####################################################

    def read_file(path):
        with open(path, "r") as f:
            content = f.read()
        return content
    
    def get_table_columns(table_schema, table_name):

        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{table_schema}'
            AND table_name = '{table_name}';
        """)
        conn.commit()

        return [a[0] for a in cursor.fetchall()]
    
    postgres2excel_columns_mapping = dict(
        patient_phone='телефон',
        patient_name='имя пациента',
        analysis_name='название анализа',
        conclusion='заключение',
    )
    
    # ------ [ Чтение скриптов ] ------ 
    
    print('Чтение скриптов...')

    sql_script_template = read_file('all_queries.template.sql')

    # ------ [ Загрузка таблицы в БД ] ------ 

    # Чтение excel-файла
    print('\tЧтение excel-файла medicine...')
    df_medicine = pd.read_excel('./medicine.xlsx', sheet_name='hard')
    lst_medicine = df_medicine.apply(lambda x: x.to_list(), axis=1).to_list()

    # Выполнение скрипта
    print('\tВыполнение скрипта...')
    fill_medicine_template_query = sql_script_template
    execute_values(cursor, sql_script_template, lst_medicine)

    print('\tПолучение резульатов...')
    list_med_results = cursor.fetchall()

    print('\tПодготовка med_results к экспорту в excel...')
    df_export_med_results = pd.DataFrame(
        list_med_results, columns=get_table_columns(table_schema='detn', table_name='ilka_med_results')
    )
    df_export_med_results = df_export_med_results.rename(postgres2excel_columns_mapping, axis=1)

    print('\tЭкспорт med_results в excel...')
    df_export_med_results.to_excel('hard_result.xlsx', index=False)

    # ------ [ Завершение ] ------ 

    print('Коммит...', end=' ')
    conn.commit()

    print('OK', end=' ')

finally:
    # Закрываем соединение
    print()
    print('Закрытие соединения...', end=' ')

    cursor.close()
    conn.close()

    print('OK')
