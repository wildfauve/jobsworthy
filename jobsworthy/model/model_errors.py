from jobsworthy.util import error

def dataframe_not_streaming():
    return error.NotAStreamError("""Streaming initialisation did not return a streaming dataframe""")
