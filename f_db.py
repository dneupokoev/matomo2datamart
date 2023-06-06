# -*- coding: utf-8 -*-
# f_db
#
# Функции для работы с базами данных
#
dv_file_version = '230606.01'
#
# 230606.01:
# + начало работы над проектом
#
# Примеры работы с clickhouse:
# https://ivan-shamaev.ru/how-to-write-data-to-clickhouse-using-python/
#
import settings
import os
import re
import sys
import json
import numpy as np
import pandas as pd
import platform
from datetime import date, datetime, timedelta
import time
import pymysql
import configparser
from clickhouse_driver import Client


#
#
#
def validate_date(date_text):
    # Проверяем корректность формата даты и диапазона
    try:
        return bool((datetime.strptime(date_text, "%Y-%m-%d")) >= datetime(2022, 1, 1, 0, 0))
    except:
        return False


#
#
#
def insert_tbl_cklickhouse(df, conn, dbname='', tblname=''):
    '''
    Функция полученный пандас.датафрейм сохраняет в таблицу кликхауса (инсерты) партиями
    '''
    dv_out_json = {}
    try:
        # Определяем названия столбцов DataFrame
        columns = df.columns
        # Отправляем данные пакетами по 50 тысяч строк
        batch_size = settings.BATCH_SIZE
        num_batches = len(df) // batch_size + 1
        for i in range(num_batches):
            start = i * batch_size
            end = (i + 1) * batch_size
            batch = df.iloc[start:end]
            data = [tuple(row) for _, row in batch.iterrows()]
            with Client(**conn) as cursor:
                cursor.execute(f'INSERT INTO {dbname}.{tblname} ({",".join(columns)}) VALUES', data)
        #
        dv_out_json['f_status'] = 'SUCCESS'
        dv_out_json['f_text'] = f"insert_tbl_cklickhouse - {tblname = } - SUCCESS - {len(df) = }"
    except Exception as error:
        dv_out_json['f_status'] = 'ERROR'
        dv_out_json['f_text'] = f"insert_tbl_cklickhouse - {tblname = } - ERROR: {error}"
    return dv_out_json


#
#
#
def prepare_tbl_dm_visits(conn, rep_date='2000-01-01', dbname=''):
    '''
    Функция подготавливает данные для таблицы dm_visits
    '''
    dv_out_json = {}
    dv_insert_json = {}
    try:
        if validate_date(rep_date) and dbname != '':
            query = (
                '''
                SELECT distinct idvisit, idsite, idvisitor, visit_last_action_time, visit_first_action_time
                FROM {dbname}.matomo_log_visit
                WHERE Date(visit_first_action_time) = {rep_date}
                '''.format(dbname=f"{dbname}", rep_date=f"'{rep_date}'")
            )
            # print(query)
            # Чтаем данные из БД
            with Client(**conn) as cursor:
                # cursor.execute("SET SESSION max_statement_time = 600")
                dv_in_row = cursor.execute(query)
                # dv_in_row = cursor.fetchall()
            #
            # Обрабатываем и подготавливаем данные
            if len(dv_in_row) > 0:
                # конвертируем в dataframe
                dv_df_src = pd.DataFrame(dv_in_row,
                                         columns=['idvisit', 'idsite', 'idvisitor', 'visit_last_action_time', 'visit_first_action_time'])
                del dv_in_row
                #
                # Удаляем данные, которые будем записывать (чтобы не задублировать)
                query = (
                    '''
                    ALTER TABLE {dbname}.dm_visits DELETE WHERE Date(visit_first_action_time) = {rep_date}
                    '''.format(dbname=f"{dbname}", rep_date=f"'{rep_date}'")
                )
                with Client(**conn) as cursor:
                    # cursor.execute("SET SESSION max_statement_time = 600")
                    dv_in_row = cursor.execute(query)
                    # dv_in_row = cursor.fetchall()
                #
                # Сохраняем подготовленные ранее данные
                dv_insert_json = insert_tbl_cklickhouse(df=dv_df_src, conn=conn, dbname=dbname, tblname='dm_visits')
                # если сохранить данные не удалось, то сразу завершаем и возвращаем причину (текст ошибки)
                if dv_insert_json['f_status'] != 'SUCCESS':
                    raise f"{dv_insert_json['f_text']}"
        dv_out_json['f_status'] = 'SUCCESS'
        dv_out_json['f_text'] = f"prepare_tbl_dm_visits - {rep_date = } - SUCCESS: {dv_insert_json['f_text']}"
    except Exception as error:
        dv_out_json['f_status'] = 'ERROR'
        dv_out_json['f_text'] = f"prepare_tbl_dm_visits - {rep_date = } - ERROR: {error}"
    return dv_out_json
