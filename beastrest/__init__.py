from flask import Flask

from .config import DefaultConfig
from .beastrest import beastrest


def create_app(config_obj="beastrest.config.DefaultConfig"):
    """Create app for Flask application
    Parameters
    ----------
    object_name: str
        the python path to the config object
        (e.g. beastrest.config.DefaultConfig)
    """

    app = Flask(__name__)

    app.config.from_object(config_obj)
    app.config.from_envvar("BEASTREST_SETTINGS", silent=True)
    app.register_blueprint(beastrest)

    app.config["CORS_HEADERS"] = "Content-Type"
    app.config["JSON_AS_ASCII"] = False
    app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True

    return app
