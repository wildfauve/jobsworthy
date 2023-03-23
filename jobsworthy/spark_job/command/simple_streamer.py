from typing import Callable, List, Set
from jobsworthy import model, repo


def run(from_table: Callable,
        to_table: Callable,
        transformer: Callable,
        write_type: model.StreamWriteType,
        from_reader_options: Set[repo.ReaderSwitch] = None,
        options: List[repo.SparkOption] = None):
    return stream_model(from_table, to_table, transformer, write_type, from_reader_options, options).run()


def stream_model(from_table: Callable,
                 to_table: Callable,
                 transformer: Callable,
                 write_type: model.StreamWriteType,
                 from_reader_options: Set[repo.ReaderSwitch],
                 options: List[repo.SparkOption]):
    return (model.Streamer().stream_from(from_table(),
                                         stream_from_reader_options=from_reader_options)
            .stream_to((to_t:= to_table()),
                       partition_columns=to_t.partition_columns,
                       write_type=write_type,
                       options=options)
            .with_transformer(transformer))
