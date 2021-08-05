class InvalidAuthentication(ValueError):
    """Raised when no info for temporary credentials or secret arn."""

    pass


class QueryFailedError(ValueError):
    """Raised when query is failed."""

    pass


class QueryAbortedError(ValueError):
    """Raised when query is aborted."""

    pass


class TableCreationError(ValueError):
    """Raised when the create table method fails"""

    pass


class DataEncodeError(ValueError):
    """Raised when the data encode fails"""

    pass


class MetadataEncodeError(ValueError):
    """Raised when the metadata escape fails"""

    pass


class InvalidRedshiftType(ValueError):
    """Raised when Redshift type is designated with unexpected string."""

    pass


class NoColumnError(ValueError):
    """Raised when input dataframe has no columns"""

    pass
