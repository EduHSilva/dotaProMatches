import os
import sqlalchemy
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def open_mariadb():
    ip = os.getenv("MARIADB_IP")
    user = os.getenv("MARIADB_USER")
    password = os.getenv("MARIADB_PWD")
    db = os.getenv("MARIADB_DBNAME")

    return sqlalchemy.create_engine(f"mysql+pymysql://{user}:{password}@{ip}/{db}?charset=utf8mb4")