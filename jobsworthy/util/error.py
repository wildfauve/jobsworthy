FAIL = 'fail'
OK = 'ok'


class JobsWorthyError(Exception):
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


class SchemaMatchingError(JobsWorthyError):
    pass


class NotAStreamError(JobsWorthyError):
    pass


class RepoConfigError(JobsWorthyError):
    pass


class RepoWriteError(JobsWorthyError):
    pass


class RepoStreamReadError(JobsWorthyError):
    pass


class StreamerTransformerError(JobsWorthyError):
    pass


class SecretError(JobsWorthyError):
    pass


class VocabNotFound(JobsWorthyError):
    pass


class ModellingException(JobsWorthyError):
    pass


class TableSchemaException(ModellingException):
    pass


class ModellingException(ModellingException):
    pass
