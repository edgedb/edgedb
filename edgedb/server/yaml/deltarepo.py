##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import os
import re

from semantix.caos.backends import deltarepo
from semantix.caos import delta as base_delta

from semantix.utils.lang import yaml


class MetaDeltaRepository(deltarepo.MetaDeltaRepository):
    def __init__(self, module, id):
        self.deltas = self._find_deltas_package(module)
        self.modhash = id

    def load_delta(self, id):
        modname = self.get_delta_module_path(id)
        mod = importlib.import_module(modname)
        return next(iter(mod.deltas))

    def load_delta_from_data(self, data):
        delta = self.load_from_string(data)
        return delta

    def write_delta_set(self, delta_set):
        path = self.get_delta_file_path(next(iter(delta_set.deltas)).id)
        with open(path, 'w') as f:
            f.write(self.dump_delta_set(delta_set))

    def delta_ref_to_id(self, ref):
        id = None

        refpath = self.get_ref_file_path(ref.ref)
        if os.path.exists(refpath):
            with open(refpath, 'r') as f:
                id = int(f.read(40), 16)
        else:
            try:
                delta_id = int(ref.ref, 16)
                deltapath = self.get_delta_file_path(delta_id)
                if os.path.exists(deltapath):
                    id = delta_id
            except ValueError:
                pass

        if id and ref.offset:
            for _ in range(ref.offset):
                delta = self.load_delta(id)
                if not delta:
                    id = None
                    break
                id = delta.parent_id
                if not id:
                    raise base_delta.DeltaRefError('unknown revision: %s' % ref)

        return id

    def update_delta_ref(self, ref, id):
        refpath = self.get_ref_file_path(ref)
        with open(refpath, 'w') as f:
            f.write('%x' % id)

    def get_ref_file_path(self, ref):
        refpath = os.path.join(self.deltas.__path__[0], 'r_%x_%s.yml' % (self.modhash, ref))
        return refpath

    def get_delta_file_path(self, delta_id):
        path = os.path.join(self.deltas.__path__[0], 'd_%x_%x.yml' % (self.modhash, delta_id))
        return path

    def remove_delta(self, delta_id):
        path = self.get_delta_file_path(delta_id)
        os.unlink(path)

    def iter_deltas(self):
        pat = r'd_%x_(?P<did>\w+)\.yml' % self.modhash

        for f in os.listdir(self.deltas.__path__[0]):
            match = re.match(pat, f)
            if match:
                yield match.group('did')

    def get_delta_module_path(self, delta_id):
        path = '%s.d_%x_%x' % (self.deltas.__name__, self.modhash, delta_id)
        return path

    def _find_deltas_package(self, module):
        paths = module.split('.')

        while paths:
            try:
                mod = importlib.import_module('.'.join(paths + ['deltas']))
                mod.caos_deltas
            except (ImportError, AttributeError):
                pass
            else:
                break
            paths.pop()

        if paths:
            return mod

    def dump_delta(self, delta):
        delta_obj = base_delta.Delta(parent_id=None, comment=None, checksum=0, deltas=[delta],
                                     formatver=base_delta.Delta.CURRENT_FORMAT_VERSION)
        delta_set = base_delta.DeltaSet([delta_obj])
        return self.dump_delta_set(delta_set)

    def dump_delta_set(self, delta_set):
        prologue = '%SCHEMA semantix.caos.backends.yaml.schemas.Delta\n---\n'
        return prologue + yaml.Language.dump(delta_set)
