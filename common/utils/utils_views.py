from flask import Blueprint, jsonify
import psycopg

from common.utils.utils import DB_CONFIG


bp_name = "utils"
bp_url_prefix = "/api/v1.0"
bp = Blueprint(bp_name, __name__, url_prefix=bp_url_prefix)


def check_database():
    try:
        with psycopg.connect(**DB_CONFIG):
            return True
    except:
        return False


@bp.route("/health", methods=["GET"])
def health_check():
    db_status = check_database()

    return jsonify(
        {
            "database": "up" if db_status else "down",
        }
    )
