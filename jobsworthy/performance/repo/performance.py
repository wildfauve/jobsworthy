from pyspark.sql import types as T
from jobsworthy import repo


class PerformanceMetric(repo.HiveRepo):
    table_name = None

    schema = (
        T.StructType()
        .add("run", T.StringType(), True)
        .add("time", T.StringType(), True)
        .add("counter", T.IntegerType(), True)
        .add("perfMetrics", T.MapType(T.StringType(), T.ArrayType(T.FloatType(), True)), True)
    )
    
    def emit_metrics(self, perf_data):
        return self.write_append(self.create_df(data=perf_data))
