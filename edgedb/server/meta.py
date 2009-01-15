import types

import semantix.caos.domain
import semantix.caos.concept
from semantix.caos import DomainClass

class MetaError(Exception):
    pass

class Bool(type):
    def __init__(self, value):
        self.value = value

class BaseMetaBackend(object):
    base_domains_to_class_map = {
                                    'str': str,
                                    'int': int,
                                    'bool': Bool
                                }

    def __init__(self):
        self.domain_backend = None
        self.semantics_backend = None
        self.semantics_cache = {}
        self.domain_cache = {}

    def load(self, type, name):
        bases = tuple()

        if type == 'domain':
            dct = {'name': name, 'constraints': {}, 'basetype': None, 'default': None}

            if isinstance(name, str) and name in self.base_domains_to_class_map:
                bases += (semantix.caos.domain.Domain, self.base_domains_to_class_map[name])
            else:
                bases, dct2 = self.domain_backend.load(name)
                dct.update(dct2)
                bases += tuple((semantix.caos.domain.Domain,))
        elif type == 'semantics':
            bases, dct = self.semantics_backend.load(name)
            bases = bases + tuple((semantix.caos.concept.Concept,))

        return bases, dct

    def store(self, cls, phase=None):
        if cls.type == 'domain':
            self.domain_backend.store(cls)
            self.domain_cache[cls.name] = cls
        elif cls.type == 'semantics':
            self.semantics_backend.store(cls, phase)
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

    def load_domain(self, name, domain):
        dct = {}
        dct['name'] = name
        dct['basetype'] = DomainClass(domain['domain'], meta_backend=self)
        dct['default'] = domain['default'] if 'default' in domain else None
        dct['constraints'] = {}

        if domain['constraints'] is not None:
            for constr in domain['constraints']:
                constr_type, constr = list(constr.items())[0]
                if isinstance(constr, str):
                    constr = constr.strip()
                self.add_domain_constraint(dct['constraints'], constr_type, constr)

        return (dct['basetype'],), dct
