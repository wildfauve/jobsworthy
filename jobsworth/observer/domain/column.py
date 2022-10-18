from jobsworth.structure.model import structure as S
from . import schema

RunTimeColumn = S.Column(vocab_term="run.sfo-lin:hasRunTime",
                         struct_fn=schema.run_time,
                         cell_builder=S.literal_time_builder)

RunColumn = S.Column(vocab_term="run",
                     struct_fn=schema.run_struct,
                     cell_builder=schema.run_builder)


DataSetInputsColumn = S.Column(vocab_term="run.sfo-lin:hasInputs",
                               struct_fn=schema.inputs_collection,
                               cell_builder=schema.inputs_builder)

OutputsColumn = S.Column(vocab_term="run.sfo-lin:hasOutputs",
                         struct_fn=schema.outputs_collection,
                         cell_builder=schema.outputs_builder)

MetricsColumn = S.Column(vocab_term="'run.sfo-lin:hasMetrics'",
                         struct_fn=schema.metrics_struct,
                         cell_builder=schema.metrics_builder)

