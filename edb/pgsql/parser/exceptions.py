from typing import Optional


class PSqlParseError(Exception):
    def __init__(self, message, lineno, cursorpos):
        self.message = message
        self.lineno = lineno
        self.cursorpos = cursorpos

    def __str__(self):
        return self.message


class PSqlUnsupportedError(Exception):
    def __init__(self, construct: Optional[str] = None):
        self.construct = construct

    def __str__(self):
        if self.construct is not None:
            return f"unsupported SQL construct: {self.construct}"
        return "unsupported SQL construct"
