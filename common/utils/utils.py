import psutil, os
import time
from typing import Dict
from flask import jsonify, abort
from pymongo import MongoClient
from functools import wraps
from flask_caching import Cache
from common.utils.logging_service import logger
from dotenv import load_dotenv

load_dotenv()

cache = Cache()
mongo = MongoClient(os.getenv("MONGO_URL"))
db = mongo["streamLineDB"]
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
        logger.info(f"Starting {func.__name__}")
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"{func.__name__} completed in {elapsed_time:.2f}s")
        print(f"Function '{func.__name__}' executed in {elapsed_time:.2f} seconds.")
        logger.info(
            f"Memory usage: {psutil.Process(os.getpid()).memory_info().rss / 1024**2:.2f} MB"
        )

        return result

    return wrapper
