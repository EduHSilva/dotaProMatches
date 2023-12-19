import requests
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
from pymongo.collection import Collection
from pymongo.database import Database
import time, datetime
import argparse


def get_matches_batch(min_match_id: int | None = None) -> list:
    """
        Captura lista de partidas pro players.
        Passando id de partida, a coleta é realizada a partir desta.
    """

    url: str = "https://api.opendota.com/api/proMatches"
    if min_match_id is not None:
        url += f"?less_than_match_id={min_match_id}"

    data: list = requests.get(url).json()
    return data


def save_matches(data: list, db_collection: Collection) -> bool:
    """
    Salva lista de partidas no banco de dados
    """
    for d in data:
        db_collection.delete_one({"match_id": d["match_id"]})
        db_collection.insert_one(d)
    return True


def get_and_save(db_collection, min_match_id=None, max_match_id=None):
    data_raw: list = get_matches_batch(min_match_id)

    data = [i for i in data_raw if "match_id" in i]

    if max_match_id is not None:
        data = [i for i in data if i["match_id"] > max_match_id]

    if len(data) == 0:
        return False, data

    save_matches(data, db_collection)
    min_match_id = min([i["match_id"] for i in data])
    time.sleep(1)
    print(len(data), " -- ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return True, data


def get_oldest_matches(db_collection: Collection) -> None:
    min_match_id_cursor = db_collection.find_one(sort=[("match_id", 1)])
    min_match_id: int | None = None
    if min_match_id_cursor is not None:
        min_match_id = min_match_id_cursor["match_id"]
    while True:
        check, data = get_and_save(db_collection, min_match_id=min_match_id)

        if not check:
            break
        min_match_id = min(i["match_id"] for i in data)


def get_newest_matches(db_collection: Collection) -> None:
    max_match_id_cursor = db_collection.find_one(sort=[("match_id", -1)])
    max_match_id: int | None = None
    if max_match_id_cursor is not None:
        max_match_id = max_match_id_cursor["match_id"]

    if max_match_id is None:
        max_match_id = 0

    _, data = get_and_save(db_collection, max_match_id=max_match_id)

    try:
        min_match_id = min([i["match_id"] for i in data])
    except ValueError:
        return

    while min_match_id > max_match_id:
        check, data = get_and_save(db_collection, min_match_id=min_match_id)

        if not check:
            break
        min_match_id = min(i["match_id"] for i in data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--how", choices=["oldest", "newest"])
    args = parser.parse_args()

    load_dotenv(find_dotenv())

    MONGODB_IP = os.getenv("MONGODB_IP")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT", 27017))

    mongodb_client: MongoClient = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database: Database = mongodb_client["dota_raw"]
    collection: Collection = mongodb_database["pro_match_history"]

    if args.how == "oldest":
        get_oldest_matches(collection)
    elif args.how == "newest":
        get_newest_matches(collection)


if __name__ == "__main__":
    main()
