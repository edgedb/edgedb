import semantix
from semantix.utils import merge

from .schema import Schema as PathSchema

class PathSpec(object):
    def __init__(self, data=None, validate=True):
        if data:
            if validate:
                self.data = PathSchema.check(data)
            else:
                self.data = data
        else:
            self.data = dict()
