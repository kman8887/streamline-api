import os
from apispec import APISpec
from flask_apispec import FlaskApiSpec
from flask_cors import CORS
from flask_talisman import Talisman
from flask import Flask
import logging
import sys

from common.utils.utils_views import bp as utils_bp
from security.auth0_service import auth0_service
from movies.movies_views import bp as movies_bp
from users.users_views import bp as users_bp, createUser
from reviews.reviews_views import bp as reviews_bp
from recommendation.recommender_views import bp as recommendation_bp
import exceptions_views
from apispec.ext.marshmallow import MarshmallowPlugin

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # Log to stdout
)

logger = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__, instance_relative_config=True)

    app.config.update(
        {
            "APISPEC_SPEC": APISpec(
                title="Streamline API",
                version="v1",
                plugins=[MarshmallowPlugin()],
                openapi_version="3.0.2",
            ),
            "APISPEC_SWAGGER_URL": "/swagger/",  # JSON
            "APISPEC_SWAGGER_UI_URL": "/swagger-ui/",  # UI
        }
    )

    app.config.update(
        CLIENT_ORIGIN_URL=os.getenv("CLIENT_ORIGIN_URL"),
        AUTH0_AUDIENCE=os.getenv("AUTH0_AUDIENCE"),
        AUTH0_DOMAIN=os.getenv("AUTH0_DOMAIN"),
        MGMT_API_ACCESS_TOKEN=os.getenv("MGMT_API_ACCESS_TOKEN"),
    )

    csp = {"default-src": ["'self'"], "frame-ancestors": ["'none'"]}
    Talisman(
        app,
        force_https=True,
        frame_options="DENY",
        content_security_policy=csp,
        referrer_policy="no-referrer",
        x_xss_protection=True,
        x_content_type_options=True,
        strict_transport_security=True,
    )

    @app.after_request
    def add_no_cache(response):
        response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    auth0_service.initialize(app.config["AUTH0_DOMAIN"], app.config["AUTH0_AUDIENCE"])

    CORS(app, origins=[os.getenv("ORIGINS")])

    app.register_blueprint(users_bp)
    app.register_blueprint(movies_bp)
    app.register_blueprint(reviews_bp)
    app.register_blueprint(recommendation_bp)
    app.register_blueprint(utils_bp)
    app.register_blueprint(exceptions_views.bp)

    app.logger.handlers = logging.getLogger().handlers
    app.logger.setLevel(logging.DEBUG)

    docs = FlaskApiSpec(app)
    docs.register(createUser, blueprint="users", endpoint="createUser")

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
