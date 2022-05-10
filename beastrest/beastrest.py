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

    res = []
    for row in df.itertuples():
        d = {}
        d["time"] = row.alarm_time.isoformat()
        d["group"] = row.groups
        d["severity_id"] = row.severity_id
        d["severity"] = row.severity
        d["status"] = row.status
        d["message"] = row.descr
        d["record"] = row.pv_name
        res.append(d)

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

    times = time.astype(int).tolist()
    res = []
    index = 0
    for row in df.itertuples():
        d = {}
        d["time"] = times[index]
        d["title"] = row.descr
        d["tags"] = row.groups
        d["text"] = row.pv_name
        res.append(d)
        index += 1

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
    eventtime = df["eventtime"].map(todt).map(jst2utc)
    alarms = df["message"].copy()
    recovers = df["message"].copy()

    alarms[df["severity"] == "OK"] = ""
    recovers[df["severity"] != "OK"] = ""

    ets = eventtime.tolist()
    alms = alarms.tolist()
    rcvs = recovers.tolist()
    res = []
    index = 0
    for row in df.itertuples():
        d = {}
        d["time"] = ets[index]
        d["group"] = row.group
        d["severity"] = row.severity
        d["status"] = row.status
        d["alarm"] = alms[index]
        d["recover"] = rcvs[index]
        d["record"] = row.record_name
        res.append(d)
        index += 1

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

    times = time.astype(int).tolist()
    res = []
    index = 0
    for row in df.itertuples():
        d = {}
        d["time"] = times[index]
        d["title"] = row.message
        d["tags"] = row.group
        d["text"] = row.severity
        res.append(d)
        index += 1

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
