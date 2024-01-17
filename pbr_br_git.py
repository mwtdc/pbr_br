#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import subprocess
import urllib
import urllib.parse
import warnings
from io import StringIO
from sys import platform
from time import sleep

import pandas as pd
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine

start_time = datetime.datetime.now()
print("--- Start PBR! 🚀 ---", str(datetime.datetime.now())[0:19])
print("ID:", int(datetime.datetime.now().timestamp()))
warnings.filterwarnings("ignore")
subprocess.run(
    "zabbix_sender -c /etc/zabbix/zabbix_agentd.conf -s Application_monitoring"
    " -k pbr_br_start -o 'Ok'",
    shell=True,
)

# Общий раздел
SESSION = requests.Session()

id_message = str(int(datetime.datetime.now().timestamp()))


# Настройки для логера
if platform == "linux" or platform == "linux2":
    logging.basicConfig(
        filename="/var/log/log-execute/log_journal_pbr.log.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(funcName)s: %(lineno)d - %(message)s"
        ),
    )
elif platform == "win32":
    logging.basicConfig(
        filename=f"{pathlib.Path(__file__).parent.absolute()}/log_journal_pbr.log.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(funcName)s: %(lineno)d - %(message)s"
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


# Функция отправки уведомлений в telegram на любое количество каналов
# (указать данные в yaml файле настроек)
def telegram(i, text):
    # Функция отправки уведомлений в telegram на любое количество каналов
    # (указать данные в yaml файле настроек)
    try:
        msg = urllib.parse.quote(str(text))
        bot_token = str(telegram_settings.bot_token[i])
        channel_id = str(telegram_settings.channel_id[i])

        retry_strategy = Retry(
            total=3,
            status_forcelist=[101, 429, 500, 502, 503, 504],
            method_whitelist=["GET", "POST"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        http.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}",
            verify=False,
            timeout=10,
        )
    except Exception as err:
        print(f"pbr_br: Ошибка при отправке в telegram - {err}")
        logging.error(f"pbr_br: Ошибка при отправке в telegram - {err}")


def connection(i):
    # Функция коннекта к базе Mysql
    # (для выбора базы задать порядковый номер числом ! начинается с 0 !)
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    db_data = f"mysql://{user_yaml}:{password_yaml}@{host_yaml}:{port_yaml}/{database_yaml}"
    return create_engine(db_data).connect()


# Функция записи датафрейма в базу
def load_data_to_db(db_name, connect_id, dataframe):
    telegram(1, "pbr_br: Старт записи в БД.")
    logging.info("pbr_br: Старт записи в БД.")

    dataframe = pd.DataFrame(dataframe)
    connection_skm = connection(connect_id)
    try:
        dataframe.to_sql(
            name=db_name,
            con=connection_skm,
            if_exists="append",
            index=False,
            chunksize=5000,
        )
        rows = len(dataframe)
        telegram(
            1,
            f"pbr_br: записано в БД {rows} строк",
        )
        logging.info(f"pbr_br: записано в БД {rows} строк")
        telegram(1, "pbr_br: Финиш записи в БД.")
        logging.info("pbr_br: Финиш записи в БД.")
    except Exception as err:
        telegram(1, f"pbr_br: Ошибка записи в БД: {err}")
        logging.info(f"pbr_br: Ошибка записи в БД: {err}")


def br_login(data):
    url = "https://br.so-ups.ru/webapi/Auth/AuthByUserName"
    while True:
        request = SESSION.post(url, data=data, verify=False)
        if request.status_code == 200:
            return request
        print(request.status_code)
        telegram(
            1,
            "pbr_br: Неуспешная авторизация на сайте br.so-ups.ru. Статус:"
            f" {request.status_code}\n ID: {id_message}",
        )
        logging.info(
            "pbr_br: Неуспешная авторизация на сайте br.so-ups.ru. Статус:"
            f" {request.status_code}\n ID: {id_message}"
        )
        sleep(5)


try:
    telegram(1, f"Старт загрузки ПБР в базу pbr_br \n ID: {id_message}")
except Exception as exc:
    print(exc)
# Получение данных по списку ГТП
date_download = str(
    (datetime.datetime.today() + datetime.timedelta(hours=1)).strftime(
        "%d.%m.%Y"
    )
)

br_login(br_settings.br_auth[0])

CsvTable = SESSION.get(
    "https://br.so-ups.ru/webapi/api/Export/Csv/Gtp.aspx?date="
    + date_download
    + "&gtpIds="
    + gtp_settings.gtp_dict[0],
    verify=False,
)
CsvTable.encoding = "windows-1251"

DataFrame = pd.DataFrame()
DataFrame = pd.read_table(StringIO(CsvTable.text), delimiter=";").fillna(0)
DataFrame.insert(
    2,
    "dt",
    pd.to_datetime(DataFrame["SESSION_DATE"], dayfirst=True, utc=False)
    + pd.to_timedelta(DataFrame["SESSION_INTERVAL"], unit="h"),
)
# print(DataFrame)

col_to_float = [
    "TG",
    "PminPDG",
    "PmaxPDG",
    "PVsvgo",
    "PminVsvgo",
    "PmaxVsvgo",
    "PminBR",
    "PmaxBR",
    "IBR",
    "CbUP",
    "CbDown",
    "CRSV",
    "TotalBR",
    "EVR",
    "OCPU",
    "OCPS",
    "Pmin",
    "Pmax",
]
for col in col_to_float:
    DataFrame[col] = (
        DataFrame[col].replace(",", ".", regex=True).astype("float")
    )

col_to_int = ["SESSION_NUMBER", "SESSION_INTERVAL"]
for col in col_to_int:
    DataFrame[col] = DataFrame[col].replace(",", ".", regex=True).astype("int")

print(DataFrame)

load_data_to_db(
    "pbr_br",
    0,
    DataFrame,
)

print("Данные ПБР в базу pbr_br загружены. 🏁")
print("Время выполнения:", str(datetime.datetime.now() - start_time)[0:10])
try:
    telegram(
        1,
        "Данные ПБР в базу pbr_br загружены."
        f" (∆={str(datetime.datetime.now() - start_time)[0:9]}\n"
        f" ID:{id_message}",
    )
except Exception as exc:
    print(exc)

# контроль в zabbix окончания работы
subprocess.run(
    "zabbix_sender -c /etc/zabbix/zabbix_agentd.conf -s Application_monitoring"
    " -k pbr_br_download_to_base -o 'Ok'",
    shell=True,
)

