from jobsworthy import structure as S
from . import vocab, schema

struct_fn = schema.inputs_collection,
cell_builder = schema.inputs_builder


def observer_table_factory() -> S.Table:
    """
    """
    table = S.Table(vocab=vocab.vocab)
    table.column_factory(vocab_term="run.sfo-lin:hasRunTime",
                         struct_fn=schema.run_time,
                         cell_builder=S.literal_time_builder)
    table.column_factory(vocab_term="run.sfo-lin:runDateUTC",
                         struct_fn=schema.run_date_utc_struct,
                         cell_builder=S.literal_date_builder)
    table.column_factory(vocab_term="run",
                         struct_fn=schema.run_struct,
                         cell_builder=schema.run_builder)
    table.column_factory(vocab_term="run.sfo-lin:hasInputs",
                         struct_fn=schema.inputs_collection,
                         cell_builder=schema.inputs_builder)
    table.column_factory(vocab_term="run.sfo-lin:hasOutputs",
                         struct_fn=schema.outputs_collection,
                         cell_builder=schema.outputs_builder)
    table.column_factory(vocab_term="run.sfo-lin:hasMetrics",
                         struct_fn=schema.metrics_struct,
                         cell_builder=schema.metrics_builder)

    return table
