import semantix
from semantix.utils import merge
from semantix.caos import DomainClass, MetaError

from .schemas.domains import Domains

class MetaData(object):
    domains = {}

    @classmethod
    def _create_class(cls, meta, dct):
        Domains.validate(meta, dct)

        base = semantix.Import(meta['class']['parent_module'], meta['class']['parent_class'])
        return type(meta['class']['name'], (base,), {'domains': merge.merge_dicts(dct, base.domains)})

class MetaDataIterator(object):
    def __init__(self, helper):
        self.helper = helper
        self.iter = iter(helper.domains)

    def __iter__(self):
        return self

    def __next__(self):
        concept = next(self.iter)
        return DomainClass(concept, meta_backend=self.helper.meta_backend)

class MetaBackendHelper(object):
    def __init__(self, metadata, meta_backend):
        self.domains = metadata.domains
        self.meta_backend = meta_backend

    def load(self, name):
        if isinstance(name, dict):
            domain = name
            name = domain['name']
        else:
            if name not in self.domains:
                raise MetaError('reference to an undefined domain: %s' % name)
            domain = self.domains[name]

        return self.meta_backend.load_domain(name, domain)

    def store(self, cls):
        pass

    def __iter__(self):
        return MetaDataIterator(self)
