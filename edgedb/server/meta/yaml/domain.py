import types
import semantix
from semantix.lib import merge
from semantix.lib.caos import cls, domain
from semantix.lib.caos.backends.meta.base import MetaError
from semantix.lib.caos.backends.meta.yaml.schemas.domains import Domains

class MetaData(object):
    domains = {}

    @classmethod
    def _create_class(cls, meta, dct):
        Domains.validate(meta['filename'])

        base = semantix.Import(meta['class']['parent_module'], meta['class']['parent_class'])
        return type(meta['class']['name'], (base,), {'domains': merge.merge_dicts(dct, base.domains)})

class MetaBackendHelper(object):
    base_domains = {
                        'str': types.UnicodeType,
                        'int': types.IntType,
                        'bool': types.BooleanType
                   }

    def __init__(self, metadata):
        self.domains = metadata.domains

    def add_constraint(self, constraints, type, value):
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

    def load(self, name):
        if name not in self.domains and name not in self.base_domains:
            raise MetaError('reference to an undefined domain: %s' % name)

        dct = {}
        dct['name'] = name

        if name in self.base_domains:
            return (self.base_domains[name],), dct

        domain = self.domains[name]

        if domain['domain'] not in self.base_domains and domain['domain'] not in self.domains:
            raise MetaError('reference to an undefined domain: %s' % domain['domain'])

        dct['basetype'] = domain.DomainClass(domain['domain'])
        dct['constraints'] = {}

        if 'constraints' in domain:
            for constr in domain['constraints']:
                constr_type, constr = constr.items()[0]
                if isinstance(constr, str):
                    constr = constr.strip()
                self.add_constraint(dct['constraints'], constr_type, constr)

        return (dct['basetype'],), dct

    def store(self, cls):
        pass
