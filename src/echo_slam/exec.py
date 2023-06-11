import os
import sqlalchemy
import datetime
import argparse
import sys 
import pandas as pd

ECHO_DIR = os.path.dirname(os.path.abspath(__file__))
DOTA_DIR = os.path.dirname(ECHO_DIR)
BASE_DIR = os.path.dirname(DOTA_DIR)
sys.path.insert(0, DOTA_DIR)

from backpack import db


def insert_data(dt_ref, query):
    query = query.format(insert = "INSERT INTO tb_vuc_safras",  date=dt_ref)
    con.execute(query)
    return True

def create_data(query, dt_ref, con):
    create_query = db.import_query(os.path.join(ECHO_DIR, "create.sql"))
    create_query = create_query.format(query.format(insert = "", date=dt_ref))
    con.execute(create_query)
    return True

date_now = datetime.datetime.now().strftime("%Y-%m-%d")
parser = argparse.ArgumentParser()
parser.add_argument("--date", help="Data para extracao", type=str)
parser.add_argument("--create", help="Define se a tabela deve ser criada", action="store_true")

args = parser.parse_args()

query = db.import_query(os.path.join(ECHO_DIR, "query.sql"))
query_fmt = query.format(dt_ref = date_now)
con = db.open_mariadb()

if args.create:
    create_data(query, args.date, con)
