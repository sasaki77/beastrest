from flask import current_app, g

from .sql import AlarmSql


def get_db():
    if "db" not in g:
        pgconfig = {
            "host": current_app.config["DATABASE_HOST"],
            "user": current_app.config["DATABASE_USER"],
            "db": current_app.config["DATABASE_DB"],
            "logdb": current_app.config["DATABASE_LOGDB"],
        }
        root = current_app.config["ROOT_COMPONENT"]
        g.db = AlarmSql(
            pgconfig["db"], pgconfig["logdb"], pgconfig["host"], pgconfig["user"], root
        )
        g.db.connect()
        g.db.update_pvlist()

    return g.db
