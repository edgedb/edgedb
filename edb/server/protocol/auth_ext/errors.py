class AuthExtError(Exception):
    """Base class for all exceptions raised by the auth extension."""
    pass

class NotFound(AuthExtError):
    """Required resource could not be found."""
    def __init__(self, description: str):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description

class MissingConfiguration(AuthExtError):
    """Required configuration is missing."""
    def __init__(self, key: str, description: str):
        self.key = key
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"key={self.key!r} "
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return f"{self.description}: {self.key}"

class InvalidData(AuthExtError):
    """Data received from the client is invalid."""
    def __init__(self, description: str):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description
