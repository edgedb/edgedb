import types
import semantix
from semantix.lib import merge
from semantix.lib.caos.domain import DomainClass
from semantix.lib.caos.backends.meta.base import MetaError
from semantix.lib.caos.backends.meta.yaml.schemas.domains import Domains

class MetaData(object):
    domains = {}

    @classmethod
    def _create_class(cls, meta, dct):
        Domains.validate(meta)

        base = semantix.Import(meta['class']['parent_module'], meta['class']['parent_class'])
        return type(meta['class']['name'], (base,), {'domains': merge.merge_dicts(dct, base.domains)})

class MetaDataIterator(object):
    def __init__(self, helper):
        self.helper = helper
        self.iter = iter(helper.domains)

    def __iter__(self):
        return self

    def next(self):
        concept = next(self.iter)
        return DomainClass(concept, meta_backend=self.helper.meta_backend)

class MetaBackendHelper(object):
    base_domains = {
                        'str': types.UnicodeType,
                        'int': types.IntType,
                        'long': types.LongType,
                        'bool': types.BooleanType
                   }

    def __init__(self, metadata, meta_backend):
        self.domains = metadata.domains
        self.meta_backend = meta_backend

    def load(self, name):
        dct = {}

        if isinstance(name, dict):
            domain = name
            name = domain['name']
        else:
            if name not in self.domains and name not in self.base_domains:
                raise MetaError('reference to an undefined domain: %s' % name)

            dct['name'] = name
            dct['basetype'] = None

            if name in self.base_domains:
                return (self.base_domains[name],), dct

            domain = self.domains[name]

        dct['name'] = name
        dct['basetype'] = None

        if name in self.base_domains:
            return (self.base_domains[name],), dct

        if domain['domain'] not in self.base_domains and domain['domain'] not in self.domains:
            raise MetaError('reference to an undefined domain: %s' % domain['domain'])

        dct['basetype'] = DomainClass(domain['domain'], meta_backend=self.meta_backend)
        dct['constraints'] = {}

        if 'constraints' in domain:
            for constr in domain['constraints']:
                constr_type, constr = constr.items()[0]
                if isinstance(constr, str):
                    constr = constr.strip()
                self.meta_backend.add_domain_constraint(dct['constraints'], constr_type, constr)

        return (dct['basetype'],), dct

    def store(self, cls):
        pass

    def __iter__(self):
        return MetaDataIterator(self)
