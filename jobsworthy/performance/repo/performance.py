from pyspark.sql import types as T
from jobsworthy import repo


class PerformanceMetric(repo.HiveRepo):
    table_name = None
    schema = T.StructType([
            T.StructField("run", T.StringType(), True),
            T.StructField("time", T.StringType(), True),
            T.StructField("counter", T.IntegerType(), True),
            T.StructField("perfMetrics", T.MapType(T.StringType(), T.ArrayType(T.FloatType(), True)), True)
        ])

    def emit_metrics(self, perf_data):
        return self.write_append(self.create_df(data=perf_data))
