##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys

from metamagic.utils.lang import javascript
from metamagic.utils.lang.javascript import adapters as jsadapters
from metamagic.utils.lang import runtimes as lang_runtimes

from metamagic.utils.fs import bucket, nodesystem, backends
from metamagic import json, node
from metamagic.node.frontends.javascript import adapters as node_adapters

from metamagic.utils.datastructures import OrderedSet

from . import classes


class BucketClassAdapter(jsadapters.BaseClassAdapter, adapts=bucket.BaseBucket):
    base_class = bucket.Bucket

    def get_dependencies(self):
        cls = self.attr_value
        deps = super().get_dependencies()
        if cls is self.base_class:
            deps.add(classes)
        return deps

    def _get_class_bases(self):
        cls = self.attr_value
        bases = super()._get_class_bases()
        if cls is self.base_class:
            bases.insert(0, 'metamagic.utils.fs.frontends.javascript.BaseBucket')
        return bases

    def get_source(self):
        cls = self.attr_value

        name = self._build_class_name_template()
        bases = self._build_class_bases_template()

        id = getattr(cls, 'id', None)

        return '''
            var id = {id};
            if (id) {{
                id = new sx.UUID(id);
            }}
            sx.define({name}, {bases}, {{ statics: {{ id: id }}}});
        '''.format(name=name, bases=bases, id=json.dumps(id))


class BackendClassAdapter(jsadapters.BaseClassAdapter, adapts=backends.Backend):
    base_class = backends.Backend

    def get_dependencies(self):
        cls = self.attr_value
        deps = super().get_dependencies()
        if cls is self.base_class:
            deps.add(classes)

        return deps

    def _get_class_bases(self):
        cls = self.attr_value
        bases = super()._get_class_bases()
        if cls is backends.FSBackend:
            bases.insert(0, 'metamagic.utils.fs.frontends.javascript.BaseFSBackend')
        return bases

    def _get_class_dict(self):
        return {}

    def _get_class_template(self):
        return '''
            sx.define({name}, {bases}, {dct});
        '''


class FSSystemClassAdapter(node_adapters.NodeSystemClassAdapter, adapts=nodesystem.FSSystem):
    def get_dependencies(self):
        cls = self.attr_value
        deps = super().get_dependencies()
        if cls is nodesystem.FSSystem:
            deps.add(classes)
        return deps

    def _get_class_bases(self):
        cls = self.attr_value
        bases = super()._get_class_bases()
        if cls is nodesystem.FSSystem:
            bases.insert(0, 'metamagic.utils.fs.frontends.javascript.BaseFSSystem')
        return bases

    def _get_class_dict(self):
        return {}

    def _get_class_template(self):
        return '''
            sx.define({name}, {bases}, {dct});
        '''


class FSBackendInstanceAdapter(javascript.JavaScriptRuntimeAdapter,
                               adapts_instances_of=backends.FSBackend):

    def collect_candidate_imports(self):
        return ()

    def get_dependencies(self):
        obj = self.attr_value
        deps = super().get_dependencies()
        basemod = lang_runtimes.load_module_for_runtime(obj.__class__.__module__,
                                                        self.runtime)
        assert basemod
        deps.append(basemod)
        return deps

    def get_source(self):
        obj = self.attr_value
        return '''new {cls}({args})'''.format(
                    cls=obj.__class__.__module__ + '.' + obj.__class__.__name__,
                    args=json.dumps(dict(
                        pub_path=obj.pub_path
                    ))
                )


class FSSystemInstanceAdapter(javascript.JavaScriptRuntimeAdapter,
                              adapts_instances_of=nodesystem.FSSystem):

    def collect_candidate_imports(self):
        return ()

    def get_dependencies(self):
        obj = self.attr_value
        deps = super().get_dependencies()

        for bucket in obj.buckets:
            backends = bucket.get_backends()
            if not backends:
                continue

            for backend in backends:
                basemod = lang_runtimes.load_module_for_runtime(backend.__class__.__module__,
                                                                self.runtime)
                assert basemod
                deps.append(basemod)

            for child_bucket in bucket._iter_children(include_self=True):
                basemod = lang_runtimes.load_module_for_runtime(child_bucket.__module__,
                                                                self.runtime)
                assert basemod
                deps.append(basemod)

        return deps

    def get_source(self):
        obj = self.attr_value

        buffer = ['var backends;']

        for bucket in obj.buckets:
            backends = bucket.get_backends()
            if not backends:
                continue

            buffer.append('backends = [];');
            for backend in backends:
                adapters = self.runtime.get_adapters(backend)
                if not adapters:
                    continue
                assert len(adapters) == 1

                adapter_cls = adapters[0]
                adapter = adapter_cls(None, 'fs_backend', backend)
                source = adapter.get_source()
                assert source

                buffer.append('backends.push({});'.format(source))

            bucket_name = bucket.__module__ + '.' + bucket.__name__

            buffer.append('if (backends.length) {{ {cls}.set_backends(backends); }}'.
                          format(cls=bucket_name))

            buffer.append('system.add_bucket({cls});'.format(cls=bucket_name))

        return '''
            (function() {{
                'use strict';

                var system = new {cls}();
                {body}
                return system;
            }})()
        '''.format(cls=obj.__class__.__module__ + '.' + obj.__class__.__name__,
                   body='\n'.join(buffer))
