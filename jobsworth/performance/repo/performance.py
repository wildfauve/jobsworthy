from pyspark.sql import types as T
from jobsworth.repo import hive_repo


class PerformanceMetric(hive_repo.HiveRepo):
    table_name = 'cbor_performance_metric'

    def emit_metrics(self, perf_data):
        return self.create(self.create_df(data=perf_data))

    def schema(self):
        """
        The default schema for the table
        :return:
        """
        return T.StructType([
            T.StructField("run", T.StringType(), True),
            T.StructField("time", T.StringType(), True),
            T.StructField("counter", T.IntegerType(), True),
            T.StructField("perfMetrics", T.MapType(T.StringType(), T.ArrayType(T.FloatType(), True)), True)
        ])

