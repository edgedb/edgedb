import semantix.lib.caos.domain
import semantix.lib.caos.concept

class MetaError(Exception):
    pass

class BaseMetaBackend(object):
    def __init__(self):
        self.domain_backend = None
        self.semantics_backend = None
        self.semantics_cache = {}
        self.domain_cache = {}

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
        if cls.type == 'domain':
            self.domain_backend.store(cls)
            self.domain_cache[cls.name] = cls
        elif cls.type == 'semantics':
            self.semantics_backend.store(cls)
            self.semantics_cache[cls.name] = cls

    def semantics(self):
        return iter(self.semantics_backend)

    def domains(self):
        return iter(self.domain_backend)

    def commit(self):
        pass

    @staticmethod
    def add_domain_constraint(constraints, type, value):
        if type == 'max-length':
            if 'max-length' in constraints:
                constraints['max-length'] = min(constraints['max-length'], value)
            else:
                constraints['max-length'] = value

        elif type == 'min-length':
            if 'min-length' in constraints:
                constraints['min-length'] = max(constraints['min-length'], value)
            else:
                constraints['min-length'] = value
        else:
            if type == 'regexp':
                value = value.replace('\\\\\\', '\\')

            if type in constraints:
                constraints[type].append(value)
            else:
                constraints[type] = [value]
