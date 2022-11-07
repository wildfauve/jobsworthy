from typing import Dict, Tuple, Optional, List

from jobsworthy.util import fn, logger

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

def term_for(path: str, vocab: Dict) -> str:
    path_array, term = term_finder(path, vocab)
    return get_term(path_array, term)


def meta_for(path: str, vocab: Dict) -> Dict:
    path_array, term = term_finder(path, vocab)
    return get_meta(term)


def term_and_meta(path: str, vocab) -> Tuple[Optional[str]]:
    path_array, term = term_finder(path, vocab)
    return get_term(path_array, term), get_meta(term)


def get_term(path_array: List[str], term: str) -> Optional[str]:
    if not term or 'hasDataProductTerm' not in term.keys():
        logger.info(msg="Vocab Term Not found", ctx={'term': term, 'path_array': path_array})
        return ".".join(path_array)
    return term.get('hasDataProductTerm', None)


def get_meta(term: str) -> Optional[str]:
    if not term or not 'hasMeta' in term.keys():
        return {}
    return term.get('hasMeta', None)


def term_finder(path, vocab):
    path_array = path.split(".")
    term = fn.deep_get(vocab, path_array)
    return path_array, term
