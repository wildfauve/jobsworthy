from enum import Enum

class VocabDirective(Enum):
    DEFAULT_WHEN_TERM_NOT_FOUND = "default_on_no_term"
    RAISE_WHEN_TERM_NOT_FOUND = "raise_on_no_term"
