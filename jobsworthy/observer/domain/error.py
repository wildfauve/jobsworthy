from jobsworthy.util import error

class ObserverError(error.JobError):
    pass

class ObserverConfigError(ObserverError):
    pass