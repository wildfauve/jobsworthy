import pytest
from jobsworthy import structure as S
from jobsworthy.util import error

def test_finds_term(vocab):
    assert S.term_for("columns.column1", vocab) == "column_one"

def test_defaults_to_path_when_term_not_found(vocab):
    assert S.term_for("not_there.column1", vocab) == "not_there.column1"

def test_raises_when_term_not_found(vocab):
    with pytest.raises(error.VocabNotFound):
        S.term_for("not_there.column1", vocab, S.raise_when_not_found) == "not_there.column1"


# Helpers
@pytest.fixture
def vocab():
    return {
        "columns": {
            "column1": {
                "term": "column_one"
            },
            "column2": {
                "term": "column_two",
                "sub1": {
                    "term": "sub_one"
                },
                "sub2": {
                    "term": "sub_two"
                },
                "sub3": {
                    "term": "sub_three",
                    "sub3-1": {
                        "term": "sub_three_one"
                    },
                    "sub3-2": {
                        "term": "sub_three_two"
                    }
                }
            }
        }
    }
