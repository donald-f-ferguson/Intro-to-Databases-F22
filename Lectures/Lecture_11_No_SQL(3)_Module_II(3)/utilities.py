import json
import pymongo
import pymysql
from sqlalchemy import create_engine
from py2neo import Graph, Node, Relationship
import csv
import pandas as pd
from nameparser import HumanName
import pymysql
import copy

_mongo_con = None
_graph = None
_engine = None
_sql_con = None


_connection_info = {
    "mongo_url": "mongodb://localhost:27017",
    "sql_alchemy_con": "mysql+pymysql://dbuser:dbuserdbuser@localhost",
    "pymysql_connect_info": {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "host": "localhost",
        "port": 3306,
        "autocommit": True,
        "cursorclass": pymysql.cursors.DictCursor
    },
    "neo4j_connect_info": {
        "url": "bolt://localhost:7687",
        "auth": ("neo4j", "dbuserdbuser")
    }
}


def get_connection_info():

    return _connection_info


def set_connection_info(c_info):

    global _connection_info

    _connection_info = copy.deepcopy(c_info)


def get_mongo_con():
    global _mongo_con

    if _mongo_con is None:
        _mongo_con = pymongo.MongoClient()

    return _mongo_con


def get_sql_engine():

    global _engine

    if _engine is None:
        _engine = create_engine(
            get_connection_info()["sql_alchemy_con"])

    return _engine


def get_mysql_con():

    global _sql_con

    if _sql_con is None:
        _sql_con = pymysql.connect(
            **get_connection_info()["pymysql_connect_info"]
        )

    return _sql_con


def get_graph():

    global _graph

    if _graph is None:
        c_info = get_connection_info()["neo4j_connect_info"]
        _graph = Graph(
            c_info["url"],
            auth=c_info["auth"]
        )

    return _graph


def run_sql(sql_string, args=None, fetch=True):

    res = None
    cur = None

    try:
        conn = get_mysql_con()
        cur = conn.cursor()

        res = cur.execute(sql_string, args)

        if fetch:
            res = cur.fetchall()
    except Exception as e:
        if cur:
            cur.close()
        raise e

    return res


def create_hw4_schema():

    sql = "create schema if not exists w4111_hw4";
    res = run_sql(sql, fetch=False)
    return res


def run_cypher(cypher_q, **kwargs):

    g = get_graph()
    res = g.run(cypher_q, **kwargs)
    return res


if __name__ == '__main__':

    g = get_graph()
    run_cypher(
        "create (:Fan { uni: $uni })",
        uni="dff9"
    )

