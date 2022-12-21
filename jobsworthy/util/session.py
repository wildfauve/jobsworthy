from typing import Callable, List, Tuple
from functools import reduce
from pyspark.sql import SparkSession

from jobsworthy.util import fn


def create_session(session_name):
    return SparkSession.builder.appName(session_name).enableHiveSupport().getOrCreate()


def build_spark_session(session_name: str, create_fn: Callable = create_session,
                        config_adder_fn: Callable = fn.identity) -> SparkSession:
    """
    Generates a Spark session object.

    + session_name: Any string describing the session.
    + create_fn: defaults to creating a standard spark session with Hive support.  To override this, provide a function with takes the
                 session name and return a Spark session
    + config_adder_fn: Defaults to noop.  A function which takes the build session and returns the built session.  It is used to
                       apply custom spark configuration.
    """
    sp = create_fn(session_name)
    config_adder_fn(sp)
    return sp


def spark_session_config(spark):
    spark.conf.set('spark.sql.jsonGenerator.ignoreNullFields', "false")


def set_session_config_options(session: SparkSession, options: List[Tuple[str, str]]) -> None:
    reduce(set_option_on_session, options, session)


def unset_session_config_options(session: SparkSession, options: List[str]) -> None:
    reduce(unset_option_on_session, options, session)


def set_option_on_session(session: SparkSession, option: Tuple[str, str]) -> SparkSession:
    session.conf.set(*option)
    return session


def unset_option_on_session(session: SparkSession, option: str) -> SparkSession:
    session.conf.unset(option)
    return session
