from jobsworthy.util import error

class ObserverError(error.JobsWorthyError):
    pass

class ObserverConfigError(ObserverError):
    pass