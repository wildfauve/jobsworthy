from typing import List
from pyspark.sql import types as T
from pyspark.sql import functions as F

from jobsworth.repo import hive_repo


class Observer(hive_repo.HiveRepo):
    table_name = "observer"

    def filter_by_inputs_run_state(self, run_state, input_locations: List[str]) -> List[T.Row]:
        df = self.read()

        if not df:
            return []

        in_location_fn = (lambda locs: lambda x: x.isin(locs))(input_locations)

        return (df.filter(
            (df.run.hasRunState == run_state) & (F.exists(df.hasInputs.hasLocation, in_location_fn)))
                .select(df.run, df.hasInputs))
