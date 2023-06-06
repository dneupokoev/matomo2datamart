_# -*- coding: utf-8 -*-
# matomo2datamart
# https://github.com/dneupokoev/matomo2datamart
#
# Matomo to DataMart: Создание витрин данных из Matomo
#
dv_file_version = '230605.01'
#
# 230605.01:
# + добавил
#
# 230605.01:
# + начало работы над проектом
#
import settings
import os
import re
import sys
import json
import platform
from datetime import date, datetime, timedelta
import time
# import pymysql
import configparser
#
from f_db import prepare_tbl_dm_visits
#
#
from pathlib import Path

try:  # from project
    dv_path_main = f"{Path(__file__).parent}/"
    dv_file_name = f"{Path(__file__).name}"
except:  # from jupiter
    dv_path_main = f"{Path.cwd()}/"
    dv_path_main = dv_path_main.replace('jupyter/', '')
    dv_file_name = 'unknown_file'

# импортируем библиотеку для логирования
from loguru import logger

# logger.add("log/" + dv_file_name + ".json", level="DEBUG", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
# logger.add("log/" + dv_file_name + ".json", level="WARNING", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
# logger.add("log/" + dv_file_name + ".json", level="INFO", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
logger.remove()  # отключаем логирование в консоль
if settings.DEBUG is True:
    logger.add(settings.PATH_TO_LOG + dv_file_name + ".log", level="DEBUG", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8")
    logger.add(sys.stderr, level="DEBUG")
else:
    logger.add(settings.PATH_TO_LOG + dv_file_name + ".log", level="INFO", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8")
    logger.add(sys.stderr, level="INFO")
logger.enable(dv_file_name)  # даем имя логированию
logger.info(f'***')
logger.info(f'BEGIN')
try:
    # Получаем версию ОС
    logger.info(f'os.version = {platform.platform()}')
except Exception as error:
    # Не удалось получить версию ОС
    logger.error(f'ERROR - os.version: {error = }')
try:
    # Получаем версию питона
    logger.info(f'python.version = {sys.version}')
except Exception as error:
    # Не удалось получить версию питона
    logger.error(f'ERROR - python.version: {error = }')
logger.info(f'{dv_file_version = }')
logger.info(f'{dv_path_main = }')
logger.info(f'{dv_file_name = }')
logger.info(f'{settings.PATH_TO_LIB = }')
logger.info(f'{settings.PATH_TO_LOG = }')
logger.info(f'{settings.DEBUG = }')
try:
    logger.info(f'{settings.EXECUTE_CLICKHOUSE = }')
    dv_EXECUTE_CLICKHOUSE = settings.EXECUTE_CLICKHOUSE
except:
    logger.info(f'settings.EXECUTE_CLICKHOUSE = None')
    dv_EXECUTE_CLICKHOUSE = True
logger.info(f'{dv_EXECUTE_CLICKHOUSE = }')
logger.info(f'{settings.BATCH_SIZE = }')
logger.info(f'{settings.WORK_MAX_MINUTES = }')


#
#
#
def get_now():
    '''
    Функция возвращает текущую дату и время в заданном формате
    '''
    logger.debug(f"get_now")
    dv_time_begin = time.time()
    dv_created = f"{datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S')}"
    # dv_created = f"{datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S.%f')}"
    return dv_created


def get_second_between_now_and_datetime(in_datetime_str='2000-01-01 00:00:00'):
    '''
    Функция возвращает количество секунд между текущим временем и полученной датой-временем в формате '%Y-%m-%d %H:%M:%S'
    '''
    logger.debug(f"get_second_between_now_and_datetime")
    tmp_datetime_start = datetime.strptime(in_datetime_str, '%Y-%m-%d %H:%M:%S')
    tmp_now = datetime.strptime(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    tmp_seconds = int((tmp_now - tmp_datetime_start).total_seconds())
    return tmp_seconds


def get_disk_space():
    '''
    Функция возвращает информацию о свободном месте на диске в гигабайтах
    dv_statvfs_bavail = Количество свободных гагабайтов, которые разрешено использовать обычным пользователям (исключая зарезервированное пространство)
    dv_statvfs_blocks = Размер файловой системы в гигабайтах
    dv_result_bool = true - корректно отработало, false - получить данные не удалось
    '''
    logger.debug(f"get_disk_space")
    dv_statvfs_blocks = 999999
    dv_statvfs_bavail = dv_statvfs_blocks
    dv_result_bool = False
    try:
        # получаем свободное место на диске ubuntu
        statvfs = os.statvfs('/')
        # Size of filesystem in bytes (Размер файловой системы в байтах)
        dv_statvfs_blocks = round((statvfs.f_frsize * statvfs.f_blocks) / (1024 * 1024 * 1024), 2)
        # Actual number of free bytes (Фактическое количество свободных байтов)
        # dv_statvfs_bfree = round((statvfs.f_frsize * statvfs.f_bfree) / (1024 * 1024 * 1024), 2)
        # Number of free bytes that ordinary users are allowed to use (excl. reserved space)
        # Количество свободных байтов, которые разрешено использовать обычным пользователям (исключая зарезервированное пространство)
        dv_statvfs_bavail = round((statvfs.f_frsize * statvfs.f_bavail) / (1024 * 1024 * 1024), 2)
        dv_result_bool = True
    except:
        pass
    return dv_statvfs_bavail, dv_statvfs_blocks, dv_result_bool


#
#
if __name__ == '__main__':
    dv_time_begin = time.time()
    # получаем информацию о свободном месте на диске в гигабайтах
    dv_disk_space_free_begin = get_disk_space()[0]
    logger.info(f"{dv_disk_space_free_begin = } Gb")
    #
    logger.info(f"{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')}")
    dv_for_send_txt_type = ''
    dv_for_send_text = ''
    dv_lib_path_ini = f"{settings.PATH_TO_LIB}/matomo2datamart.cfg"
    dv_cfg_last_run_is_success = 0
    dv_cfg = configparser.ConfigParser()
    dv_cfg_last_send_tlg_success = '2000-01-01 00:00:00'
    try:
        # читаем значения из конфига
        if os.path.exists(dv_lib_path_ini):
            with open(dv_lib_path_ini, mode="r", encoding='utf-8') as fp:
                dv_cfg.read_file(fp)
        # читаем значения
        dv_cfg_last_send_tlg_success = dv_cfg.get('DEFAULT', 'last_send_tlg_success', fallback='2000-01-01 00:00:00')
        dv_cfg_last_run_is_success = dv_cfg.get('DEFAULT', 'last_run_is_success', fallback=1)
        logger.info(f"read cfg success")
        logger.info(f"{dv_cfg_last_send_tlg_success = }")
        logger.info(f"{dv_cfg_last_run_is_success = }")
    except:
        logger.error(f"read cfg error")
        pass
    #
    try:
        dv_file_lib_path = f"{settings.PATH_TO_LIB}/matomo2datamart.dat"
        if os.path.exists(dv_file_lib_path):
            dv_file_lib_open = open(dv_file_lib_path, mode="r", encoding='utf-8')
            dv_file_lib_time = next(dv_file_lib_open).strip()
            dv_file_lib_open.close()
            dv_file_old_start = datetime.strptime(dv_file_lib_time, '%Y-%m-%d %H:%M:%S')
            tmp_now = datetime.strptime(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
            tmp_seconds = int((tmp_now - dv_file_old_start).total_seconds())
            if tmp_seconds < settings.WORK_MAX_MINUTES * 2 * 60:
                raise Exception(f"Уже выполняется c {dv_file_lib_time} - перед запуском дождитесь завершения предыдущего процесса!")
        else:
            dv_file_lib_open = open(dv_file_lib_path, mode="w", encoding='utf-8')
            dv_file_lib_time = f"{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}"
            dv_file_lib_open.write(f"{dv_file_lib_time}")
            dv_file_lib_open.close()
        #
        #
        # данные для таблицы dm_visits
        logger.debug(f"prepare_tbl_dm_visits - begin")
        dv_result_json = prepare_tbl_dm_visits(conn=settings.CH_connect, rep_date='2023-05-01', dbname=settings.CH_matomo_dbname)
        logger.debug(f"prepare_tbl_dm_visits - {dv_result_json = }")
        logger.debug(f"prepare_tbl_dm_visits - end")
        if dv_result_json['f_status'] != 'SUCCESS':
            raise f"{dv_result_json['f_text']}"
        #
        # если заливка данных в кликхаус прошла успешно, то делаем отметку об этом
        dv_for_send_txt_type = dv_result_json['f_status']
        dv_cfg_last_run_is_success = '1'
        os.remove(dv_file_lib_path)
    except Exception as ERROR:
        dv_cfg_last_run_is_success = '0'
        dv_for_send_txt_type = 'ERROR'
        dv_for_send_text = f"{ERROR = }"
    finally:
        # сохраняем информацию о результате работы скрипта (успешно/ошибка)
        try:
            dv_cfg.set('DEFAULT', 'last_run_is_success', dv_cfg_last_run_is_success)
        except Exception as ERROR:
            logger.error(f"{ERROR = }")
            pass
        # получаем информацию о свободном месте на диске в гигабайтах
        dv_disk_space_free_end, dv_statvfs_blocks, dv_result_bool = get_disk_space()
        logger.info(f"{dv_disk_space_free_end = } Gb")
        try:
            if settings.CHECK_DISK_SPACE is True:
                # формируем текст о состоянии места на диске
                if dv_result_bool is True:
                    dv_for_send_text = f"{dv_for_send_text} | disk space all/free_begin/free_end: {dv_statvfs_blocks}/{dv_disk_space_free_begin}/{dv_disk_space_free_end} Gb"
                else:
                    dv_for_send_text = f"{dv_for_send_text} | check_disk_space = ERROR"
        except:
            pass
        #
        if dv_for_send_txt_type == 'ERROR':
            logger.error(f"{dv_for_send_text}")
        else:
            logger.info(f"{dv_for_send_text}")
        try:
            if settings.SEND_TELEGRAM is True:
                if dv_for_send_txt_type == 'ERROR':
                    # если отработало с ошибкой, то в телеграм оправляем ошибку ВСЕГДА! хоть каждую минуту - это не спам
                    dv_is_SEND_TELEGRAM_success = True
                else:
                    dv_is_SEND_TELEGRAM_success = False
                    # Чтобы слишком часто не спамить в телеграм сначала проверим разрешено ли именно сейчас отправлять сообщение
                    try:
                        # проверяем нужно ли отправлять успех в телеграм
                        if get_second_between_now_and_datetime(dv_cfg_last_send_tlg_success) > settings.SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES * 60:
                            dv_is_SEND_TELEGRAM_success = True
                            # актуализируем значение конфига
                            dv_cfg_last_send_tlg_success = get_now()
                            dv_cfg.set('DEFAULT', 'last_send_tlg_success', dv_cfg_last_send_tlg_success)
                        else:
                            dv_is_SEND_TELEGRAM_success = False
                    except:
                        dv_is_SEND_TELEGRAM_success = False
                #
                # пытаться отправить будем только если предыдущие проверки подтвердили необходимость отправки
                if dv_is_SEND_TELEGRAM_success is True:
                    settings.f_telegram_send_message(tlg_bot_token=settings.TLG_BOT_TOKEN, tlg_chat_id=settings.TLG_CHAT_FOR_SEND,
                                                     txt_name=f"matomo2datamart {dv_file_version}",
                                                     txt_type=dv_for_send_txt_type,
                                                     txt_to_send=f"{dv_for_send_text}",
                                                     txt_mode=None)
                    logger.info(f"send to telegram: {dv_for_send_text}")
        except:
            pass
        try:
            # сохраняем файл конфига
            with open(dv_lib_path_ini, mode='w', encoding='utf-8') as configfile:
                dv_cfg.write(configfile)
        except:
            pass
        #
        logger.info(f"{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')}")
        work_time_ms = int('{:.0f}'.format(1000 * (time.time() - dv_time_begin)))
        logger.info(f"{work_time_ms = }")
        #
        logger.info(f'END')
