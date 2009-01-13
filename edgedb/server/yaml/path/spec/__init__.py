import semantix
from semantix.utils import merge

from .schema import Schema

class PathSpec(object):
    data = {}

    @classmethod
    def _create_class(cls, meta, dct):
        Schema.validate(meta, dct)

        base = semantix.Import(meta['class']['parent_module'], meta['class']['parent_class'])
        return type(meta['class']['name'], (base,), {'data': merge.merge_dicts(dct, base.data)})
