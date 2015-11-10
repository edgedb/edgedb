##
# Copyright (c) 2013-2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys

from metamagic import json
from metamagic import node

from metamagic.node.frontends.jplus import adapters as node_adapters

from importkit import runtimes as lang_runtimes
from metamagic.utils.lang import jplus
from metamagic.utils.lang.jplus import adapters as jpadapters
from metamagic.utils.lang.jplus import uuid as jp_uuid

from metamagic.utils.datastructures import OrderedSet
from metamagic.utils.fs import bucket, nodesystem, backends

from . import classes


class BucketClassAdapter(jpadapters.BaseClassAdapter, adapts=bucket.BaseBucket):
    base_class = bucket.BaseBucket

    def collect_candidate_imports(self):
        cls = self.attr_value
        imps = super().collect_candidate_imports()
        if cls is self.base_class:
            imps.add(classes)
        imps.add(jp_uuid)
        return imps

    def _get_class_bases(self):
        cls = self.attr_value
        bases = super()._get_class_bases()
        if cls is self.base_class:
            bases.insert(0, 'metamagic.utils.fs.frontends.jplus.classes.BaseBucket')
        return bases

    def _get_class_dict(self):
        cls = self.attr_value
        id = getattr(cls, 'id', None)

        if id:
            return {
                'id': 'new metamagic.utils.lang.jplus.uuid.UUID("{}")'.format(id)
            }
        else:
            return {}


class BackendClassAdapter(jpadapters.BaseClassAdapter, adapts=backends.Backend):
    base_class = backends.Backend

    def collect_candidate_imports(self):
        cls = self.attr_value
        imps = super().collect_candidate_imports()
        if cls is self.base_class:
            imps.add(classes)

        return imps

    def _get_class_bases(self):
        cls = self.attr_value
        bases = super()._get_class_bases()
        if cls is backends.Backend:
            bases.insert(0, 'metamagic.utils.fs.frontends.jplus.classes.BaseFSBackend')
        return bases

    def _get_class_dict(self):
        return {}


class FSSystemClassAdapter(node_adapters.NodeSystemClassAdapter,
                           adapts=nodesystem.FSSystem):
    def collect_candidate_imports(self):
        cls = self.attr_value
        imps = super().collect_candidate_imports()
        if cls is nodesystem.FSSystem:
            imps.add(classes)
        return imps

    def _get_class_bases(self):
        cls = self.attr_value
        bases = super()._get_class_bases()
        if cls is nodesystem.FSSystem:
            bases.insert(0, 'metamagic.utils.fs.frontends.jplus.classes.BaseFSSystem')
        return bases

    def _get_class_dict(self):
        return {}


class FSBackendInstanceAdapter(jplus.JPlusWebRuntimeAdapter,
                               adapts_instances_of=backends.BaseFSBackend):

    def collect_candidate_imports(self):
        imps = OrderedSet()

        obj = self.attr_value
        basemod = sys.modules[obj.__class__.__module__]
        imps.add(basemod)

        return imps

    def get_jplus_source(self):
        obj = self.attr_value
        return 'new {cls}({args})'.format(
                   cls=obj.__class__.__module__ + '.' + obj.__class__.__name__,
                   args=json.dumps(dict(
                       pub_path=getattr(obj, 'pub_path', None)
                   ))
               )


class FSSystemInstanceAdapter(jplus.JPlusWebRuntimeAdapter,
                              adapts_instances_of=nodesystem.FSSystem):

    def collect_candidate_imports(self):
        obj = self.attr_value
        imps = OrderedSet()

        for bucket in obj.buckets:
            backends = bucket.get_backends()
            if not backends:
                continue

            for backend in backends:
                basemod = sys.modules[backend.__class__.__module__]
                imps.add(basemod)

            for child_bucket in bucket._iter_children(include_self=True):
                basemod = sys.modules[child_bucket.__module__]
                imps.add(basemod)

        return imps

    def get_jplus_source(self):
        obj = self.attr_value

        buffer = []

        for bucket in obj.buckets:
            backends = bucket.get_backends()
            if not backends:
                continue

            buffer.append('backends = []')
            for backend in backends:
                adapters = self.runtime.get_adapters(backend)
                if not adapters:
                    continue
                assert len(adapters) == 1

                adapter_cls = adapters[0]
                adapter = adapter_cls(None, 'fs_backend', backend)
                source = adapter.get_jplus_source()
                assert source

                buffer.append('backends.push({})'.format(source))

            bucket_name = bucket.__module__ + '.' + bucket.__name__

            buffer.append(
                'if backends.length {{ {cls}.setBackends(backends) }}'.
                          format(cls=bucket_name))

            buffer.append('system.addBucket({cls});'.format(cls=bucket_name))

        return '''
            (fn() {{
                system = new {cls}()
                {body}
                return system
            }})()
        '''.format(cls=obj.__class__.__module__ + '.' + obj.__class__.__name__,
                   body='\n'.join(buffer))
