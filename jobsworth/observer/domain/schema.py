from typing import Tuple, List
from pyspark.sql.types import StructType, StringType

from ...structure import schema_util as su
from . import vocab as V


def run_time(term, vocab):
    return su.build_string_field(
        term,
        vocab,
        nullable=False)


def run_date_utc_struct(term, vocab):
    return su.build_string_field(term, vocab, nullable=False)


def ds_input_struct():
    return StructType([
        su.at_id,
        su.at_type,
        su.build_string_field('run.sfo-lin:hasInputs.sfo-lin:hasLocation', V.vocab, nullable=False),
        su.build_string_field('run.sfo-lin:hasInputs.sfo-lin:hasName', V.vocab, nullable=True),
    ])


def inputs_collection(term, vocab):
    return su.build_array_field(term, vocab, ds_input_struct(), nullable=True)


def ds_output_struct():
    return StructType([
        su.at_id,
        su.at_type,
        su.build_string_field('run.sfo-lin:hasOutputs.sfo-lin:hasLocation', V.vocab, nullable=False),
        su.build_string_field('run.sfo-lin:hasOutputs.sfo-lin:hasName', V.vocab, nullable=True),
    ])


def outputs_collection(term, vocab):
    return su.build_array_field(term, vocab, ds_output_struct(), nullable=True)


def run_struct(term, vocab):
    return su.build_struct_field(
        term,
        vocab,
        StructType([
            su.at_id,
            su.at_type,
            su.build_string_field('run.sfo-lin:isRunOf', V.vocab, nullable=False),
            su.build_string_field('run.sfo-lin:hasTrace', V.vocab, nullable=True),
            su.build_string_field('run.sfo-lin:hasStartTime', V.vocab, nullable=False),
            su.build_string_field('run.sfo-lin:hasEndTime', V.vocab, nullable=True),
            su.build_string_field('run.sfo-lin:hasRunState', V.vocab, nullable=True)
        ]),
        nullable=False)


def metrics_struct(term, vocab):
    return su.build_array_field(
        term,
        vocab,
        StringType(),
        nullable=True)


def run_builder(cell) -> Tuple[str, str, str, str, str, str, str]:
    return (cell.props['id'],
            cell.props['type'],
            cell.props['jobId'],
            cell.props['traceId'],
            cell.props['startTime'],
            cell.props['endTime'],
            cell.props['state'])


def inputs_builder(cell) -> List[Tuple[str, str, str, str]]:
    return cell.props['listOfInput']


def outputs_builder(cell) -> List[Tuple[str, str, str, str]]:
    return cell.props['listOfOutputs']


def metrics_builder(cell) -> List[str]:
    return cell.props['listOfMetrics']
