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
    base_atoms_to_class_map = {
                                  'str': str,
                                  'int': int,
                                  'bool': Bool
                              }

    def __init__(self):
        self.cache = {}

    def is_atom(self, name):
        return name in self.base_atoms_to_class_map

    def get_atom_skeleton(self, name):
        bases = tuple()

        dct = {'name': name, 'mods': {}, 'base': None, 'default': None}

        return bases, dct

    def load(self, name):
        bases = tuple()

        if self.is_atom(name):
            bases, dct = self.get_atom_skeleton(name)

            if name in self.base_atoms_to_class_map:
                bases += (semantix.caos.domain.Domain, self.base_atoms_to_class_map[name])
            else:
                bases, dct2 = self.do_load(name)
                dct.update(dct2)
                bases += tuple((semantix.caos.domain.Domain,))
        else:
            bases, dct = self.do_load(name)
            bases = bases + tuple((semantix.caos.concept.Concept,))

        return bases, dct

    def concepts(self):
        return self.iter_concepts()

    def atoms(self):
        return self.iter_atoms()

    def commit(self):
        pass

    @staticmethod
    def add_atom_mod(mods, type, value):
        if type == 'max-length':
            if 'max-length' in mods:
                mods['max-length'] = min(mods['max-length'], value)
            else:
                mods['max-length'] = value

        elif type == 'min-length':
            if 'min-length' in mods:
                mods['min-length'] = max(mods['min-length'], value)
            else:
                mods['min-length'] = value
        else:
            if type == 'regexp':
                value = value.replace('\\\\\\', '\\')

            if type in mods:
                mods[type].append(value)
            else:
                mods[type] = [value]

    def load_atom(self, atom):
        dct = {}
        dct['name'] = atom['name']
        dct['base'] = DomainClass(atom['extends'], meta_backend=self)

        if 'default' in atom and atom['default'] is not None:
            dct['default'] = dct['base'](atom['default'])
        else:
            dct['default'] = None

        dct['mods'] = {}

        if atom['mods'] is not None:
            for mod in atom['mods']:
                mod_type, mod = list(mod.items())[0]
                if isinstance(mod, str):
                    mod = mod.strip()
                self.add_atom_mod(dct['mods'], mod_type, mod)

        return (dct['base'],), dct
