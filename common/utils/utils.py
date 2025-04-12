import os
import time
from typing import Dict
from flask import jsonify, abort
from pymongo import MongoClient
from functools import wraps
from flask_caching import Cache

cache = Cache()
mongo = MongoClient(os.getenv("MONGO_URL"))
db = mongo["streamLineDB"]
movies_db = db["movies"]
users_db = db.users
letterboxd_interactions = db["letterboxd_interactions"]
user_recommendations = db["user_recommendations"]


DB_CONFIG: Dict[str, str] = {
    "dbname": "streamline",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": "5432",
}


def json_abort(status_code, data=None):
    response = jsonify(data)
    response.status_code = status_code
    abort(response)


def time_it(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Function '{func.__name__}' executed in {elapsed_time:.2f} seconds.")
        return result

    return wrapper
