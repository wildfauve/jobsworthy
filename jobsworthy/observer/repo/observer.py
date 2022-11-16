from typing import List
from pyspark.sql import types as T
from pyspark.sql import functions as F

from jobsworthy import repo


class Observer(repo.HiveRepo):
    table_name = "observer"

    partition_columns = ("hasRunDateUTC",)

    pruning_column = 'hasRunDateUTC'

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.run.id = {update_name}.run.id"

    def filter_by_inputs_run_state(self, run_state, input_locations: List[str]) -> List[T.Row]:
        df = self.read()

        if not df:
            return []

        in_location_fn = (lambda locs: lambda x: x.isin(locs))(input_locations)

        return (df.filter(
            (df.run.hasRunState == run_state) & (F.exists(df.hasInputs.hasLocation, in_location_fn)))
                .select(df.run, df.hasInputs))
