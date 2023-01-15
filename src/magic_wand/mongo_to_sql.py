import pandas as pd
import sqlalchemy
from pymongo import MongoClient
import os
import dotenv

def parse_match(match_data):
    new_data = {}
    for c in match_data:
        try:
            _ = len(match_data[c])
        except TypeError:
            new_data[c] = [match_data[c]]

    return pd.DataFrame(new_data)

def parse_player(player_data):
    columns = ["match_id", "player_slot", "account_id", "assists", "deaths", "denies", "firstblood_claimed",
                "gold", "gold_per_min", "gold_spent", "hero_damage", "hero_healing", "hero_id", "last_hits", "level",
                #"max_hero_hit",
                "pred_vict", "roshans_killed", "tower_damage", "towers_killed", "xp_per_min",
                "personaname", "name", "radiant_win", "start_time", "duration", "patch", "region", "win", "total_gold", "total_xp", 
                "kills_per_min", "kda", "neutral_kills", "game_mode", "tower_kills", "courier_kills", "lane_kills", "hero_kills", "observer_kills",
                "sentry_kills", "roshan_kills", "necronomicon_kills", "ancient_kills", "buyback_count", "observer_uses", "sentry_uses",
                "lane_efficiency", "lane_efficiency_pct", "lane", "lane_role", "purchase_tpscroll", "actions_per_min", "rank_tier"]

    df_standard = pd.DataFrame(columns=columns)
    df = pd.DataFrame( {c: [player_data[c]] for c in player_data if c in columns})
    df["dt_match"] = pd.to_datetime(df["start_time"], unit="s")
    df = pd.concat([df_standard, df], ignore_index=True)
    columns = df.columns.to_list()
    columns.sort()
    return df[columns]

def get_players(match_data):
    dfs = []
    for p in match_data["players"]:
        dfs.append(parse_player(p))
    
    if len(dfs) == 0:
        return None

    return pd.concat(dfs)

def insert_players(data, con):
    data.to_sql("tb_match_player", con, if_exists="append", index=False)
    return True

def open_connection_mariadb():
    ip = os.getenv("MARIADB_IP")
    user = os.getenv("MARIADB_USER")
    password = os.getenv("MARIADB_PWD")
    db = os.getenv("MARIADB_DBNAME")

    return sqlalchemy.create_engine(f"mysql+pymysql://{user}:{password}@{ip}/{db}?charset=utf8mb4")

def get_match_list(con, collection):
    query = "SELECT DISTINCT match_id AS id_list FROM tb_match_player"
    matchs_id_db = pd.read_sql_query(query, con)["id_list"].tolist()
    match_list = collection.find({"match_id": {"$nin": [matchs_id_db]}})
    return match_list

def main():
    dotenv.load_dotenv(dotenv.find_dotenv())

    con = open_connection_mariadb()

    MONGODB_IP = os.getenv("MONGODB_IP")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT", 27017))
    mongodb_client: MongoClient = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database = mongodb_client["dota_raw"]

    details_collection = mongodb_database["pro_match_details"]
    cursor = get_match_list(con, details_collection)
    for c in cursor:
        df_players = get_players(c)
        if df_players is None:
            continue
        insert_players(df_players, con)

if __name__ == "__main__":
    main()