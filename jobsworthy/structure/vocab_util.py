from typing import Dict, Tuple, Optional, List, Callable, Union

from .value import VocabDirective
from jobsworthy.util import fn, logger, error

TERM_NAMES = {'hasDataProductTerm', 'term'}
"""
This is a vocab mapping module.  The vocab is laid out in a tree structure aligned, at the root to each of the column-level concepts
within the CBOR.  The branches are specific routes into the concept.  There is a special branch defined as "*" which is used for concepts
whose vocab is common across concepts.

Use the term_for function to determine the name in the table for all nodes in a tree.  For example, the following:

> StructField(V.term_for("*.fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasAmount"), DecimalType(20, 6), True),

defines a dataframe structured field for the leaf node fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasAmount.  That maps to 
{'hasDataProductTerm': "amount"}, which means that the name of that property in the structure will be "amount".  Where as the tree path 
helps us understand the Ontology path to the concept.  

"""

def default_to_path(path_array: List[str], term: str, vocab_directives):
    if VocabDirective.RAISE_WHEN_TERM_NOT_FOUND in vocab_directives:
        return raise_when_not_found(path_array, term, vocab_directives)
    logger.info(msg="Vocab Term Not found", ctx={'term': term, 'path_array': path_array})
    return ".".join(path_array)

def raise_when_not_found(path_array: List[str], _term: str, vocab_directives):
    raise error.VocabNotFound(f"Vocab with path '{'.'.join(path_array)}' not found")

def term_for(path: str,
             vocab: Union[Dict, Tuple[List, Dict]],
             not_found_strategy: Callable = default_to_path) -> str:
    """
    Takes a path, a string using a simple dot notation to separate the vocab dict path, and a vocab dictionary and
    returns a translated term name.  The vocab is a Dict without lists in the following format:
        {
            "columns": {
                "column1": {
                    "term": "column_one"
                },
                "column2": {
                    "term": "column_two",
                    "sub1": {
                        "term": "sub_one"
                    }
                }
            }
        }
    The path "columns.column2.sub1" returns the term "sub_one"
    :param path:
    :param vocab:
    :param not_found_strategy:  The function to call when the path does not return a term.  The default is to return
                                the path as a term name (default_to_path).  Otherwise use raise_when_not_found, which
                                will call raise.
    :return: Optional[str].  Possible Exception when using raise_when_not_found.
    """
    vocab_directives, vocab_dict = extract_vocab_directives(vocab)
    path_array, term = term_finder(path, vocab_dict)
    return _get_term(path_array, term, not_found_strategy, vocab_directives)


def meta_for(path: str, vocab: Union[Dict, Tuple[List, Dict]]) -> Dict:
    vocab_directives, vocab_dict = extract_vocab_directives(vocab)
    path_array, term = term_finder(path, vocab_dict)
    return get_meta(term)


def term_and_meta(path: str,
                  vocab: Union[Dict, Tuple[List, Dict]],
                  not_found_strategy: Callable = default_to_path) -> Tuple[Optional[str]]:

    vocab_directives, vocab_dict = extract_vocab_directives(vocab)
    path_array, term = term_finder(path, vocab_dict)
    return _get_term(path_array, term, not_found_strategy, vocab_directives), get_meta(term)


def _get_term(path_array: List[str],
              term: str,
              not_found_strategy: Callable,
              vocab_directives) -> Optional[str]:
    if not term or TERM_NAMES.isdisjoint(set(term.keys())):
        return not_found_strategy(path_array, term, vocab_directives)
    return term.get((TERM_NAMES & (set(term.keys()))).pop(), None)


def get_meta(term: str) -> Optional[str]:
    if not term or not 'hasMeta' in term.keys():
        return {}
    return term.get('hasMeta', None)



def term_finder(path, vocab):
    path_array = path.split(".")
    term = fn.deep_get(vocab, path_array)
    return path_array, term


def extract_vocab_directives(vocab: Union[Dict, Tuple[List, Dict]]) -> Tuple:
    if isinstance(vocab, tuple):
        return vocab
    return [], vocab
