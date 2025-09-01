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

    sql_queries = dict()
    sql_queries['loading'] = dict()
    sql_queries['processing'] = dict()
    sql_queries['export'] = dict()

    # загрузка medicine
    sql_queries['loading']['drop_medicine'] = read_file(
        'sql_queries/loading/1_load_medicine/1_1_drop_medicine.sql'
    )
    sql_queries['loading']['create_medicine'] = read_file(
        'sql_queries/loading/1_load_medicine/1_2_create_medicine.sql'
    )
    sql_queries['loading']['fill_medicine.template'] = read_file(
        'sql_queries/loading/1_load_medicine/1_3_fill_medicine.template.sql'
    )

    # binary_report_mapping
    sql_queries['processing']['drop_binary_report_mapping'] = read_file(
        'sql_queries/processing/1_binary_report_mapping/1_1_drop_binary_report_mapping.sql'
    )
    sql_queries['processing']['create_binary_report_mapping'] = read_file(
        'sql_queries/processing/1_binary_report_mapping/1_2_create_binary_report_mapping.sql'
    )
    sql_queries['processing']['fill_binary_report_mapping'] = read_file(
        'sql_queries/processing/1_binary_report_mapping/1_3_fill_binary_report_mapping.sql'
    )

    # full_report
    sql_queries['processing']['drop_full_report'] = read_file(
        'sql_queries/processing/2_full_report/2_1_drop_full_report.sql'
    )
    sql_queries['processing']['create_full_report'] = read_file(
        'sql_queries/processing/2_full_report/2_2_create_full_report.sql'
    )

    # med_results
    sql_queries['processing']['drop_med_results'] = read_file(
        'sql_queries/processing/3_med_results/3_1_drop_med_results.sql'
    )
    sql_queries['processing']['create_med_results'] = read_file(
        'sql_queries/processing/3_med_results/3_2_create_med_results.sql'
    )

    # экспорт med_results
    sql_queries['export']['select_med_results'] = read_file(
        'sql_queries/export/1_select_med_results.sql'
    )

    # ------ [ Загрузка таблицы в БД ] ------ 

    print('Работа с medicine:')

    # Чтение excel-файла
    print('\tЧтение excel-файла medicine...')
    df_medicine = pd.read_excel('./medicine.xlsx', sheet_name='hard')

    # Удаление старой таблицы
    print('\tУдаление старой таблицы medicine...')
    cursor.execute(sql_queries['loading']['drop_medicine'])

    # Создание новой таблицы
    print('\tСоздание новой таблицы medicine...')
    cursor.execute(sql_queries['loading']['create_medicine'])

    # Заполение таблицы
    print('\tЗаполение таблицы medicine...')
    lst_medicine = df_medicine.apply(lambda x: x.to_list(), axis=1).to_list()
    fill_medicine_template_query = sql_queries['loading']['fill_medicine.template']
    execute_values(cursor, fill_medicine_template_query, lst_medicine)

    # ------ [ Таблица сопоставления бинарных значений заключения ] ------ 

    print('Работа с binary_report_mapping:')

    # Удаление старой таблицы binary_report_mapping
    print('\tУдаление старой таблицы binary_report_mapping...')
    cursor.execute(sql_queries['processing']['drop_binary_report_mapping'])

    # Создание новой таблицы binary_report_mapping
    print('\tСоздание новой таблицы binary_report_mapping...')
    cursor.execute(sql_queries['processing']['create_binary_report_mapping'])

    # Заполение таблицы binary_report_mapping
    print('\tЗаполение таблицы binary_report_mapping...')
    cursor.execute(sql_queries['processing']['fill_binary_report_mapping'])

    # ------ [ Полный отчет ] ------ 

    print('Работа с full_report:')

    # Удаление старой таблицы full_report
    print('\tУдаление старой таблицы full_report...')
    cursor.execute(sql_queries['processing']['drop_full_report'])

    # Создание новой таблицы full_report
    print('\tСоздание новой таблицы full_report...')
    cursor.execute(sql_queries['processing']['create_full_report'])

    # ------ [ Отфильрованный отчет ] ------ 

    print('Работа с med_results:')

    # Удаление старой таблицы med_results
    print('\tУдаление старой таблицы med_results...')
    cursor.execute(sql_queries['processing']['drop_med_results'])

    # Создание новой таблицы med_results
    print('\tСоздание новой таблицы med_results...')
    cursor.execute(sql_queries['processing']['create_med_results'])

    # ------ [ Отфильрованный отчет ] ------ 

    print('Экспорт med_results в excel:')

    print('\tПолучение med_results из БД...')
    cursor.execute(sql_queries['export']['select_med_results'])
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
