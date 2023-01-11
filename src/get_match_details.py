import requests
from pymongo import MongoClient
import os
import dotenv
from pymongo.collection import Collection
from pymongo.database import Database
from tqdm import tqdm
import time, datetime

def get_data(match_id) -> list:
    url:str = f"https://api.opendota.com/api/matches/{match_id}"
    data = requests.get(url).json()
    return data

def save_data(data, db_collection):
    try:
        db_collection.delete_one({"match_id":data["match_id"]})
        db_collection.insert(data)
        return True
    except KeyError:
        return False

def find_matches_ids(mongo_db_database):
    collection_history = mongo_db_database["pro_match_history"]
    collection_details = mongo_db_database["pro_match_details"]
    match_history = set([i["match_id"] for i in collection_history.find({}, {"match_id": 1})])
    match_details = set([i["match_id"] for i in collection_details.find({}, {"match_id": 1})])
    match_ids = list(match_history - match_details)
    return match_ids

def main():
    MONGODB_IP = os.getenv("MONGODB_IP")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT", 27017))
    mongodb_client: MongoClient = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database: Database = mongodb_client["dota_raw"]
    for match_id in tqdm(find_matches_ids(mongodb_database)):
        data = get_data(match_id)
        if(save_data(data, mongodb_database["pro_match_details"])):
            time.sleep(1)
        else:
            print(data)
            time.sleep(60)

if __name__ == "__main__":
    main()
