import semantix.lib.caos.domain
import semantix.lib.caos.concept

class MetaError(Exception):
    pass

class BaseMetaBackend(object):
    def __init__(self):
        self.domain_backend = None
        self.semantics_backend = None

    def load(self, type, name):
        bases = tuple()

        if type == 'domain':
            bases, dct = self.domain_backend.load(name)
            bases = bases + tuple((semantix.lib.caos.domain.Domain,))
        elif type == 'semantics':
            bases, dct = self.semantics_backend.load(name)
            bases = bases + tuple((semantix.lib.caos.concept.Concept,))

        return bases, dct

    def store(self, cls):
        raise NotImplementedError()
