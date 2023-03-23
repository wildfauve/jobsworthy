from typing import Optional, List, Dict, Tuple
from enum import Enum
from collections import ChainMap

from jobsworthy.util import fn


class SparkOption(Enum):
    """
    Option is a generator for options used on Spark functions.  In some cases spark options come in 2 flavours, which
    can be the same or different.
    + The declaration used on read and write functions.  This is a Dictionary
    + The declaration applied to the spark session.  This is a Tuple.
    """
    MERGE_SCHEMA = ({'mergeSchema': 'true'},
                    ('spark.databricks.delta.schema.autoMerge.enabled', 'true'))
    COSMOS_INFER_SCHEMA = ({"spark.cosmos.read.inferSchema.enabled": "true"},)
    COSMOS_ITEM_OVERWRITE = ({"spark.cosmos.write.strategy": "ItemOverwrite"},)
    COSMOS_READ_PARTITION_DEFAULT = ({"spark.cosmos.read.partitioning.strategy": "Default"},)
    COSMOS_CHANGE_FEED_INCREMENTAL = ({"spark.cosmos.changeFeed.mode": "Incremental"},)

    @classmethod
    def function_based_options(cls, options) -> Dict:
        opts = [] if not options else options
        return dict(ChainMap(*[cls._declaration(o, False) for o in opts]))

    @classmethod
    def _declaration(cls, option, for_session: bool = True):
        if not for_session:
            return option.value[0]

        if len(option.value) == 2:
            return option.value[1]
        return list(option.value[0].items())[0]


    @classmethod
    def options_to_spark_options(cls, options) -> Optional[List[Tuple[str, str]]]:
        """
        Delta tables don't have an options function which would allow setting mergeSchema to true.  The Delta approach
        is to set the spark session conf param 'spark.databricks.delta.schema.autoMerge.enabled' to 'true'.

        This ONLY caters for mergeSchema option.

        self.db.session.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
        :param session:
        :param options: A collection of Option
        :return:
        """
        return [cls._declaration(opt) for opt in
                fn.select(lambda option: cls._declaration(option), options)]

    @classmethod
    def options_to_spark_option_names(cls, options) -> Optional[List[Tuple[str, str]]]:
        """
        self.db.session.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
        :param session:
        :param options: A collection of Option
        :return:
        """
        return [cls._declaration(opt)[0] for opt in
                fn.select(lambda option: cls._declaration(option), options)]
