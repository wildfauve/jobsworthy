from typing import Callable
from pyspark.sql import SparkSession

from jobsworth.util import fn

def create_session(session_name):
    return SparkSession.builder.appName(session_name).enableHiveSupport().getOrCreate()


def build_spark_session(session_name: str, create_fn: Callable = create_session, config_adder_fn: Callable = fn.identity) -> SparkSession:
    sp = create_fn(session_name)
    config_adder_fn(sp)
    return sp


def spark_session_config(spark):
    spark.conf.set('spark.sql.jsonGenerator.ignoreNullFields', "false")
