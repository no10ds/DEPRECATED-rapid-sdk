class SchemaAlreadyExistsException(Exception):
    pass


class JobFailedException(Exception):
    def __init__(self, message, data):
        self.message = message
        self.data = data


class UnableToFetchJobStatusException(Exception):
    def __init__(self, message, data):
        self.message = message
        self.data = data


class DataFrameUploadFailedException(Exception):
    def __init__(self, message, data):
        self.message = message
        self.data = data


class SchemaGenerationFailedException(Exception):
    def __init__(self, message, data):
        self.message = message
        self.data = data


class SchemaCreateFailedException(Exception):
    def __init__(self, message, data):
        self.message = message
        self.data = data


class CannotFindCredentialException(Exception):
    pass


class AuthenticationErrorException(Exception):
    pass


class SchemaInitialisationException(Exception):
    pass
