FAIL = 'fail'
OK = 'ok'


class JobError(Exception):
    """
    Base Error Class for Job errors
    """

    def __init__(self, message="", name="", ctx={}, code=500, klass="", retryable=False, traceback: str = None):
        self.code = 500 if code is None else code
        self.retryable = retryable
        self.message = message
        self.name = name
        self.ctx = ctx
        self.klass = klass
        self.traceback = traceback
        super().__init__(self.message)

    def error(self):
        return {'error': self.message, 'code': self.code, 'step': self.name, 'ctx': self.ctx}


class SchemaMatchingError(JobError):
    pass


class NotAStreamError(JobError):
    pass


class RepoConfigError(JobError):
    pass


class RepoWriteError(JobError):
    pass


class SecretError(JobError):
    pass


class VocabNotFound(JobError):
    pass


class ModellingException(JobError):
    pass
