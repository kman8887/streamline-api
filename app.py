import os
from flask_cors import CORS
from flask_talisman import Talisman
from flask import Flask

from security.auth0_service import auth0_service
from movies.movies_views import bp as movies_bp
from users.users_views import bp as users_bp
from reviews.reviews_views import bp as reviews_bp
from recommendation.recommender_views import bp as recommendation_bp
import exceptions_views
from flask_cors import CORS


def create_app():
    # use fastAPI instead
    app = Flask(__name__, instance_relative_config=True)

    app.config["CLIENT_ORIGIN_URL"] = "http://localhost:4200"
    app.config["AUTH0_AUDIENCE"] = "http://localhost:5000"
    app.config["AUTH0_DOMAIN"] = os.getenv("AUTH0_DOMAIN")
    app.config["MGMT_API_ACCESS_TOKEN"] = os.getenv("MGMT_API_ACCESS_TOKEN")

    csp = {"default-src": ["'self'"], "frame-ancestors": ["'none'"]}

    Talisman(
        app,
        force_https=False,
        frame_options="DENY",
        content_security_policy=csp,
        referrer_policy="no-referrer",
        x_xss_protection=False,
        x_content_type_options=True,
    )

    auth0_service.initialize(app.config["AUTH0_DOMAIN"], app.config["AUTH0_AUDIENCE"])

    @app.after_request
    def add_headers(response):
        response.headers["X-XSS-Protection"] = "0"
        response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
        return response

    CORS(app, origins=os.getenv("ORIGINS"))

    app.register_blueprint(users_bp)
    app.register_blueprint(movies_bp)
    app.register_blueprint(reviews_bp)
    app.register_blueprint(recommendation_bp)
    app.register_blueprint(exceptions_views.bp)

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
