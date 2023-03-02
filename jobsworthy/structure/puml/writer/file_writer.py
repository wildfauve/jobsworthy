from typing import Tuple, List, Dict, Union
from pathlib import Path
import json

from jobsworthy.util import console


def general_writer(output_tuples: List[Tuple[str, Union[List, Dict]]]):
    [_generic_write(output) for output in output_tuples]
    pass


def write(schema_file_name: str,
          vocab_file_name: str,
          repo_file_name: str,
          schema: List,
          vocab: Dict,
          repo: List) -> None:
    general_writer([(schema_file_name, schema), (vocab_file_name, vocab), (repo_file_name, repo)])


def _generic_write(output: Tuple[str, Union[List, Dict]]):
    file_name, to_write = output
    if isinstance(to_write, dict):
        _write_dict(file_name, to_write)
        return
    if isinstance(to_write, list):
        _write_list(file_name, to_write)
        return


def _write_list(file, to_write):
    if not file:
        return
    console.console.print(f"Writing to: {file}", style="blue")
    with open(Path(file), 'w') as writer:
        writer.writelines("\n".join(to_write))


def _write_dict(file, to_write):
    if not file:
        return
    console.console.print(f"Writing to: {file}", style="blue")
    with open(Path(file), 'w') as writer:
        writer.write(json.dumps(to_write, indent=4))
