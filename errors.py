"""mongoreader.errors

Here error classes for mongoreader are defined."""


class MongoreaderException(Exception):
    """Base class for errors in this module."""
    pass


class ImplementationError(MongoreaderException):
    """Error raised when something is not implemented correctly in mongoreader.
    
    For example, this exception is raised by waferCollation.__init__ when
    arguments are not passed correctly."""

    def __init__(self, message):
        super().__init__(message)