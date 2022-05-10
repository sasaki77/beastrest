import re

import pandas as pd
from pandas.io.sql import DatabaseError
import psycopg2

from .sqlstate import *


class AlarmSql(object):
    def __init__(self, dbname, logdbname, host, user, root):
        self.dbname = dbname
        self.host = host
        self.dbuser = user
        self.root = root
        self.logdbname = logdbname

        self.conn_alm = None
        self.conn_log = None

        self.pvlist = None

    def connect(self):
        self.conn_alm = psycopg2.connect(dbname=self.dbname,
                                         host=self.host,
                                         user=self.dbuser)
        self.conn_alm.autocommit = True

        self.conn_log = psycopg2.connect(dbname=self.logdbname,
                                         host=self.host,
                                         user=self.dbuser)
        self.conn_log.autocommit = True

    def close(self):
        if self.conn_alm:
            self.conn_alm.close()

        if self.conn_log:
            self.conn_log.close()

        print("close")

    def current_alarm_all(self):
        sql_str = SQL_CURRENT_ALARM_ALL.format(self.root)
        try:
            data = pd.read_sql(sql=sql_str, con=self.conn_alm)
        except (ValueError, DatabaseError):
            raise
        return data

    def current_alarm_msg(self, msg):
        sql_str = SQL_CURRENT_ALARM_MSG.format(self.root, msg)
        try:
            data = pd.read_sql(sql=sql_str, con=self.conn_alm)
        except (ValueError, DatabaseError):
            raise
        return data

    def history_alarm_all(self, message, starttime, endtime):
        # with message filter
        if message and message != ".*":
            try:
                mask = self.pvlist["message"].str.contains(message)
                pvlist = self.pvlist[mask]
            except re.error:
                return []
            # id, datum, record_name, severity, eventtime, status

            sql_str = SQL_HISTORY_GROUP.format(self.root)
            params = (starttime, endtime, pvlist["record_name"].tolist())

            try:
                data = pd.read_sql(sql=sql_str, con=self.conn_log,
                                   params=params)
            except (ValueError, DatabaseError):
                raise

            ret = data.merge(self.pvlist)

            return ret.sort_values(by="id", ascending=False)

        # without message filter
        # id, datum, record_name, severity, eventtime, status
        sql_str = SQL_HISTORY_ALL.format(self.root)
        params = (starttime, endtime)

        try:
            data = pd.read_sql(sql=sql_str, con=self.conn_log,
                               params=params)
        except (ValueError, DatabaseError):
            raise

        ret = data.merge(self.pvlist, how="left")
        ret["group"] = ret["group"].fillna("Unknown")
        ret["message"] = ret["message"].fillna("Unknown")

        return ret.sort_values(by="id", ascending=False)

    def history_alarm_group(self, group, message, starttime, endtime):
        try:
            mask = self.pvlist["group"].str.match(group, na=False)
            pvlist = self.pvlist[mask]
            pvlist = pvlist[pvlist["message"].str.contains(message)]
        except re.error:
            return []

        # id, datum, record_name, severity, eventtime, status
        sql_str = SQL_HISTORY_GROUP.format(self.root)
        params = (starttime, endtime, pvlist["record_name"].tolist())

        try:
            data = pd.read_sql(sql=sql_str, con=self.conn_log, params=params)
        except (ValueError, DatabaseError):
            raise

        ret = data.merge(self.pvlist)

        return ret.sort_values(by="id", ascending=False)

    def update_pvlist(self):
        # record_name, message, group, sub_group, sub_sub_group
        sql_str = SQL_PV_LIST.format(self.root)
        try:
            df = pd.read_sql(sql=sql_str, con=self.conn_alm)
        except (ValueError, DatabaseError):
            df = self.pvlist

        df["group"] = (df["group"] + df["sub_group"].apply(self._sgstr)
                       + df["sub_sub_group"].apply(self._sgstr))

        self.pvlist = df.drop(["sub_group", "sub_sub_group"], axis=1)

    def _sgstr(self, sg_str):
        return " / " + sg_str if sg_str else ""
