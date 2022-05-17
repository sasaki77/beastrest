import re
from datetime import datetime
from dateutil import tz

import pandas as pd
from pandas.io.sql import DatabaseError
import psycopg2
from flask import Blueprint, request, jsonify
from werkzeug.exceptions import BadRequestKeyError

from .db import get_db

beastrest = Blueprint("beastrest", __name__)


@beastrest.route("/", methods=["GET"])
def hello():
    return "CSS Alarm JSON test"


@beastrest.route("/current", methods=["GET"])
def current():
    entity = request.args.get("entity", ".*")
    msg = request.args.get("message", "")

    try:
        df = get_current_alarm(entity, msg)
    except (ValueError, DatabaseError):
        msg = "RDB Error: entity = {}, msg = {}".format(entity, msg)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 500
    except re.error as e:
        msg = "regex error ({}) entity = {}, msg = {}"
        msg = msg.format(e, entity, msg)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 500

    df["alarm_time"] = df["alarm_time"].map(jst2utc)
    df = df.drop(columns=["sub_group", "sub_sub_group", "severity_id"])
    df = df.rename(
        columns={
            "alarm_time": "time",
            "groups": "group",
            "descr": "message",
            "pv_name": "record",
        }
    )
    res = df.to_dict(orient="records")

    return jsonify(res)


@beastrest.route("/current/ann", methods=["GET"])
def get_current_ann():
    entity = request.args.get("entity", ".*")
    msg = request.args.get("message", "")

    try:
        df = get_current_alarm(entity, msg)
    except (ValueError, DatabaseError):
        msg = "RDB Error: entity = {}, msg = {}".format(entity, msg)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 500
    except re.error as e:
        msg = "regex error ({}) entity = {}, msg = {}"
        msg = msg.format(e, entity, msg)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 500

    if df["alarm_time"].empty:
        time = df["alarm_time"]
    else:
        time = df["alarm_time"].dt.strftime("%s%f").str[:-3]

    df["alarm_time"] = time.astype(int).tolist()
    df = df.drop(
        columns=[
            "group",
            "sub_group",
            "sub_sub_group",
            "severity_id",
            "severity",
            "status",
        ]
    )
    df = df.rename(
        columns={
            "alarm_time": "time",
            "descr": "title",
            "groups": "tags",
            "pv_name": "text",
        }
    )
    res = df.to_dict(orient="records")

    return jsonify(res)


@beastrest.route("/history", methods=["GET"])
def history():
    group = request.args.get("entity", "all")
    msg = request.args.get("message", "")

    try:
        start, end = get_time_from_arg(request.args)
    except (BadRequestKeyError, ValueError):
        print("Error: Invalid argumets")
        msg = "Arguments Error: starttime or endtime are invalid"
        msg += ". args = " + str(request.args)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 400

    try:
        df = get_history_alarm(group, msg, start, end)
    except psycopg2.Error:
        temp = "RDB Error: entity = {}, msg = {}," "starttime = {}, endtime={}"
        msg = temp.format(group, msg, start, end)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 500

    # Drop lines if it has NaN value
    df = df.dropna()

    todt = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
    df["eventtime"] = df["eventtime"].map(todt).map(jst2utc)
    alarms = df["message"].copy()
    recovers = df["message"].copy()

    alarms[df["severity"] == "OK"] = ""
    recovers[df["severity"] != "OK"] = ""

    df["alarm"] = alarms.tolist()
    df["recover"] = recovers.tolist()

    df = df.drop(columns=["id", "datum", "message"])
    df = df.rename(columns={"record_name": "record", "eventtime": "time"})
    res = df.to_dict(orient="records")

    return jsonify(res)


@beastrest.route("/history/ann", methods=["GET"])
def get_history_ann():
    group = request.args.get("entity", "all")
    msg = request.args.get("message", "")
    svr = request.args.get("severity", "")

    try:
        start, end = get_time_from_arg(request.args)
    except (BadRequestKeyError, ValueError):
        print("Error: Invalid argumets")
        msg = "Arguments Error: starttime or endtime are invalid"
        msg += ". args = " + str(request.args)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 400

    try:
        df = get_history_alarm(group, msg, start, end)
    except psycopg2.Error:
        temp = "RDB Error: entity = {}, msg = {}," "starttime = {}, endtime={}"
        msg = temp.format(group, msg, start, end)
        ret = {"value": False, "descriptor": msg}
        return jsonify(ret), 500

    # Drop lines if it has NaN value
    df = df.dropna()

    df = df[df["severity"].str.match(svr)]

    df["eventtime"] = pd.to_datetime(df["eventtime"])
    if df["eventtime"].empty:
        time = df["eventtime"]
    else:
        time = df["eventtime"].dt.strftime("%s%f").str[:-3]

    df["eventtime"] = time.astype(int).tolist()
    df = df.drop(columns=["id", "datum", "record_name", "status"])
    df = df.rename(
        columns={
            "eventtime": "time",
            "message": "title",
            "group": "tags",
            "severity": "text",
        }
    )
    res = df.to_dict(orient="records")

    return jsonify(res)


def iso_to_dt(iso_str):
    try:
        dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        JST = tz.gettz("Asia/Tokyo")
        UTC = tz.gettz("UTC")
        dt_jst = dt.replace(tzinfo=UTC).astimezone(JST)
        return dt_jst
    except ValueError:
        raise


def jst2utc(x):
    JST = tz.gettz("Asia/Tokyo")
    UTC = tz.gettz("UTC")
    dt_utc = x.replace(tzinfo=JST).astimezone(UTC)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sgstr(sg_str):
    return " / " + sg_str if sg_str else ""


def entity2re(entity):
    name_to_match = entity.replace("(", r"\(")
    name_to_match = name_to_match.replace(")", r"\)")
    name_to_match = name_to_match.replace("{", "(")
    name_to_match = name_to_match.replace("}", ")")
    name_to_match = name_to_match.replace(",", "|")
    return name_to_match


def get_time_from_arg(arg):
    try:
        starttime = arg["starttime"]
        endtime = arg["endtime"]
    except BadRequestKeyError:
        raise

    # id, datum, record_name, severity, eventtime, status, group, message
    try:
        start = iso_to_dt(starttime)
        end = iso_to_dt(endtime)
    except ValueError:
        raise

    return start, end


def get_current_alarm(group, msg):
    # "alarm_time", "group", "sub_group", "sub_sub_group"
    # "severity", "status", "descr", "pv_name", "severity_id"
    try:
        rdb = get_db()
        if msg:
            df = rdb.current_alarm_msg(msg)
        else:
            df = rdb.current_alarm_all()
    except (ValueError, DatabaseError):
        raise

    df["groups"] = (
        df["group"] + df["sub_group"].apply(sgstr) + df["sub_sub_group"].apply(sgstr)
    )
    try:
        name_to_match = entity2re(group)
        filtered_df = df[df["groups"].str.match(name_to_match)]
    except re.error:
        raise

    return filtered_df


def get_history_alarm(group, msg, start, end):
    # "id", "datum", "record_name", "severity",
    # "eventtime", "status", "message", "group"
    try:
        rdb = get_db()
        if group == "all":
            df = rdb.history_alarm_all(msg, start, end)
        else:
            name_to_match = entity2re(group)
            df = rdb.history_alarm_group(name_to_match, msg, start, end)
    except (ValueError, DatabaseError):
        raise

    return df
