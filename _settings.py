# -*- coding: utf-8 -*-
# matomo2datamart
# https://github.com/dneupokoev/matomo2datamart
#
# Matomo to DataMart: Создание витрин данных из Matomo
#
# 230605:
# + базовая версия (начало работы над проектом)
#
#
# ВНИМАНИЕ!!! Перед запуском необходимо ЗАПОЛНИТЬ пароли в данном файле и ПЕРЕИМЕНОВАТЬ его в settings.py
#
# подключение к mysql (matomo)
MySQL_matomo_host = '192...'
MySQL_matomo_port = 3306
MySQL_matomo_dbname = 'matomo'
MySQL_matomo_user = ''
MySQL_matomo_password = ''
MySQL_matomo_charset = 'utf8mb4'
#
CH_matomo_host = '192...'
CH_matomo_port = 9000
CH_matomo_dbname = 'matomo'
CH_matomo_user = ''
CH_matomo_password = ''
CH_matomo_charset = 'UTF-8'
#
#
#
# *** Настройки ***
# для избыточного логирования True (нужно для тестирования и отладки), иначе False
DEBUG = True
# DEBUG = False
#
# EXECUTE_CLICKHOUSE - True: выполнять insert в ClickHouse (боевой режим); False: не выполнять insert (для тестирования и отладки)
EXECUTE_CLICKHOUSE = True
# EXECUTE_CLICKHOUSE = False
#
# создаем папку для логов:
# sudo mkdir /var/lib/matomo2datamart
# выдаем полные права на папку:
# sudo chmod 777 /var/lib/matomo2datamart
PATH_TO_LIB = '/var/lib/matomo2datamart/'
#
# создаем папку для переменных данного проекта:
# sudo mkdir /var/log/matomo2datamart
# выдаем полные права на папку:
# sudo chmod 777 /var/log/matomo2datamart
PATH_TO_LOG = '/var/log/matomo2datamart/'
#
#
# Какое максимальное количество строк обрабатывать за один вызов скрипта
# BATCH_SIZE - общее количество строк (ВНИМАНИЕ! минимум 1!!! иначе ничего обрабатывать не будет)
BATCH_SIZE = 50000
#
# максимальное количество минут работы скрипта до остановки (0 - без остановки, int - может понадобиться, чтобы гибче управлять автозапуском)
# ВНИМАНИЕ! реально может работать на несколько минут дольше указанного
WORK_MAX_MINUTES = 10
#
#
#
#
# True - Проверять свободное место на диске, False - не проверять
CHECK_DISK_SPACE = True
#
#
# TELEGRAM
# True - отправлять результат в телеграм, False - не отправлять
SEND_TELEGRAM = True
# SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES - минимальное количество минут между отправками УСПЕХА (чтобы не заспамить)
SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES = 360
# создать бота - получить токен - создать группу - бота сделать администратором - получить id группы
TLG_BOT_TOKEN = 'token'
# TLG_CHAT_FOR_SEND = идентификатор группы
# Как узнать идентификтор группы:
# 1. Добавить бота в нужную группу;
# 2. Написать хотя бы одно сообщение в неё;
# 3. Отправить GET-запрос по следующему адресу:
# curl https://api.telegram.org/bot<your_bot_token>/getUpdates
# 4. Взять значение "id" из объекта "chat". Это и есть идентификатор чата. Для групповых чатов он отрицательный, для личных переписок положительный.
TLG_CHAT_FOR_SEND = -0
# TLG_CHAT_FOR_SEND = 0
#
#
#
#
#
#
#
#
#
#
#
# ВНИМАНИЕ!!! Дальше настройки работы. Перед тем как их трогать НЕОБХОДИМО разобраться в настройках и понимать что к чему!
#
MySQL_connect = {'host': MySQL_matomo_host,
                 'port': MySQL_matomo_port,
                 'user': MySQL_matomo_user,
                 'passwd': MySQL_matomo_password,
                 'charset': MySQL_matomo_charset}
#
CH_connect = {'host': CH_matomo_host,
              'port': CH_matomo_port,
              'database': CH_matomo_dbname}
#
#
#
#
import telebot


def f_telegram_send_message(tlg_bot_token='', tlg_chat_id=None, txt_to_send='', txt_mode=None, txt_type='', txt_name=''):
    '''
    Функция отправляет в указанный чат телеграма текст
    Входные параметры: токен, чат, текст, тип форматирования текста (HTML, MARKDOWN)
    '''
    if txt_type == 'ERROR':
        txt_type = '❌'
        # txt_type = '\u000274C'
    elif txt_type == 'WARNING':
        txt_type = '⚠'
        # txt_type = '\U0002757'
    elif txt_type == 'INFO':
        txt_type = 'ℹ'
        # txt_type = '\U0002755'
    elif txt_type == 'SUCCESS':
        txt_type = '✅'
        # txt_type = '\U000270'
    else:
        txt_type = ''
    txt_to_send = f"{txt_type} {txt_name} | {txt_to_send}"
    try:
        # dv_tlg_bot = telebot.TeleBot(TLG_BOT_TOKEN, parse_mode=None)
        # dv_tlg_bot = telebot.TeleBot(TLG_BOT_TOKEN, parse_mode='MARKDOWN')
        dv_tlg_bot = telebot.TeleBot(tlg_bot_token, parse_mode=txt_mode)
        # отправляем текст
        tmp_out = dv_tlg_bot.send_message(tlg_chat_id, txt_to_send[0:3999])
        return f"chat_id = {tlg_chat_id} | message_id = {tmp_out.id} | html_text = '{tmp_out.html_text}'"
    except Exception as error:
        return f"ERROR: {error}"
