#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import urllib
import urllib.parse
import warnings
from io import StringIO
from time import sleep

import pandas as pd
import pymysql
import requests
import yaml

start_time = datetime.datetime.now()
warnings.filterwarnings("ignore")


# Настройки для логера
logging.basicConfig(
    filename="log_journal_pbr.log",
    level=logging.INFO,
    format=(
        "%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s"
    ),
)
# Загружаем yaml файл с настройками
with open(
    str(pathlib.Path(__file__).parent.absolute()) + "/settings.yaml", "r"
) as yaml_file:
    settings = yaml.safe_load(yaml_file)
telegram_settings = pd.DataFrame(settings["telegram"])
sql_settings = pd.DataFrame(settings["sql_db"])
br_settings = pd.DataFrame(settings["br"])
gtp_settings = pd.DataFrame(settings["gtp"])

# Функция отправки уведомлений в telegram на любое количество каналов (указать данные в yaml файле настроек)


def telegram(text):
    msg = urllib.parse.quote(str(text))
    for channel in range(len(telegram_settings.index)):
        bot_token = str(telegram_settings.bot_token[channel])
        channel_id = str(telegram_settings.channel_id[channel])

        requests.adapters.DEFAULT_RETRIES = 5
        s = requests.session()
        s.keep_alive = False

        s.post(
            "https://api.telegram.org/bot"
            + bot_token
            + "/sendMessage?chat_id="
            + channel_id
            + "&text="
            + msg,
            timeout=10,
        )


# Функция коннекта к базе Mysql (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)


def connection(i):
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    return pymysql.connect(
        host=host_yaml,
        user=user_yaml,
        port=port_yaml,
        password=password_yaml,
        database=database_yaml,
    )


id_message = str(int(datetime.datetime.now().timestamp()))
try:
    telegram("Старт загрузки ПБР в базу pbr_br \n ID: " + id_message)
except Exception as exc:
    print(exc)
# Получение данных по списку ГТП
date_download = str(
    (datetime.datetime.today() + datetime.timedelta(hours=1)).strftime(
        "%d.%m.%Y"
    )
)

AuthByUserName = requests.post(
    "https://br.so-ups.ru/webapi/Auth/AuthByUserName/",
    data=br_settings.br_auth[0],
    verify=False,
)

if AuthByUserName.status_code == 200:
    print(AuthByUserName.status_code)
if AuthByUserName.status_code != 200:
    try:
        telegram(
            "Неуспешная авторизация на сайте br.so-ups.ru. Статус: "
            + str(AuthByUserName.status_code)
            + "\n ID: "
            + id_message
        )
    except Exception as exc:
        print(exc)
    while AuthByUserName.status_code != 200:
        AuthByUserName = requests.post(
            "https://br.so-ups.ru/webapi/Auth/AuthByUserName/",
            data=br_settings.br_auth[0],
            verify=False,
        )
        print(AuthByUserName.status_code)
        sleep(5)

CsvTable = requests.get(
    "https://br.so-ups.ru/webapi/api/Export/Csv/Gtp.aspx?date="
    + date_download
    + "&gtpIds="
    + gtp_settings.gtp_dict[0],
    cookies=AuthByUserName.cookies,
    verify=False,
)
CsvTable.encoding = "windows-1251"

DataFrame = pd.DataFrame()
DataFrame = pd.read_table(StringIO(CsvTable.text), delimiter=";").fillna(0)
DataFrame["dt"] = pd.to_datetime(
    DataFrame["SESSION_DATE"], dayfirst=True, utc=False
) + pd.to_timedelta(DataFrame["SESSION_INTERVAL"], unit="h")

connection_vc = connection(0)
conn_cursor = connection_vc.cursor()

vall = ""
rows = len(DataFrame.index)
gtp_rows = int(round(rows / 24, 0))
for r in range(len(DataFrame.index)):
    vall = (
        vall
        + "('"
        + str(DataFrame.GTP_ID[r])
        + "','"
        + str(DataFrame.GTP_NAME[r])
        + "','"
        + str(DataFrame.dt[r])
        + "','"
        + str(DataFrame.SESSION_DATE[r])
        + "','"
        + str(DataFrame.SESSION_NUMBER[r])
        + "','"
        + str(DataFrame.SESSION_INTERVAL[r])
        + "','"
        + str(DataFrame.TG[r]).replace(",", ".")
        + "','"
        + str(DataFrame.PminPDG[r]).replace(",", ".")
        + "','"
        + str(DataFrame.PmaxPDG[r]).replace(",", ".")
        + "','"
        + str(DataFrame.PVsvgo[r]).replace(",", ".")
        + "','"
        + str(DataFrame.PminVsvgo[r]).replace(",", ".")
        + "','"
        + str(DataFrame.PmaxVsvgo[r]).replace(",", ".")
        + "','"
        + str(DataFrame.PminBR[r]).replace(",", ".")
        + "','"
        + str(DataFrame.PmaxBR[r]).replace(",", ".")
        + "','"
        + str(DataFrame.IBR[r]).replace(",", ".")
        + "','"
        + str(DataFrame.CbUP[r]).replace(",", ".")
        + "','"
        + str(DataFrame.CbDown[r]).replace(",", ".")
        + "','"
        + str(DataFrame.CRSV[r]).replace(",", ".")
        + "','"
        + str(DataFrame.TotalBR[r]).replace(",", ".")
        + "','"
        + str(DataFrame.EVR[r]).replace(",", ".")
        + "','"
        + str(DataFrame.OCPU[r]).replace(",", ".")
        + "','"
        + str(DataFrame.OCPS[r]).replace(",", ".")
        + "','"
        + str(DataFrame.Pmin[r]).replace(",", ".")
        + "','"
        + str(DataFrame.Pmax[r]).replace(",", ".")
        + "'"
        + "),"
    )

vall = vall[:-1]
sql = (
    "INSERT INTO pbr_br.pbr_br"
    " (GTP_ID,GTP_NAME,dt,SESSION_DATE,SESSION_NUMBER,SESSION_INTERVAL,TG,PminPDG,PmaxPDG,PVsvgo,PminVsvgo,PmaxVsvgo,PminBR,PmaxBR,IBR,CbUP,CbDown,CRSV,TotalBR,EVR,OCPU,OCPS,Pmin,Pmax)"
    " VALUES "
    + vall
    + ";"
)
conn_cursor.execute(sql)
connection_vc.commit()
connection_vc.close()
print("Данные ПБР в базу pbr_br загружены. 🏁")
print("Время выполнения:", str(datetime.datetime.now() - start_time)[0:10])
try:
    telegram(
        "Данные ПБР в базу pbr_br загружены. 🏁"
        + "  ("
        + " ∆="
        + (str(datetime.datetime.now() - start_time)[0:9])
        + ") \n ID: "
        + id_message
    )
except Exception as exc:
    print(exc)
