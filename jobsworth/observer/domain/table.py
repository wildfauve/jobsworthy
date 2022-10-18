from jobsworth.structure.model import structure as S
from . import column as C


def observer_table_factory() -> S.Table:
    """
    """
    return S.Table(columns=[C.RunTimeColumn,
                            C.RunColumn,
                            C.DataSetInputsColumn,
                            C.OutputsColumn,
                            C.MetricsColumn])
