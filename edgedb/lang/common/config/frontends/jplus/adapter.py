##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys

from metamagic import json

from jplus.compiler.environment import metamagic as jplus

from metamagic.utils.config import configurable, cvalue
from metamagic.utils.config.frontends import FrontendConfigModule


MOD_TPL = '''\
{exports}


{classes}
'''

CLS_TPL = '''\
class {clsname} {{
    {cvars}
}}
'''


class FrontendConfigModuleAdapter(jplus.JPlusWebRuntimeAdapter,
                                  adapts_instances_of=FrontendConfigModule):

    def get_jplus_source(self):
        module = self.module

        classes = []
        exports = []

        for cls in module.__dict__.values():
            if isinstance(cls, configurable.ConfigurableMeta):
                if cls.__module__ != module.__name__:
                    continue

                cvars = []

                for attrname, attr in cls.__dict__.items():
                    if isinstance(attr, cvalue):
                        cval = getattr(cls, attrname)
                        cvars.append((attrname, json.dumps(cval)))

                cvarstext = '\n'.join('{} = {}'.format(n, v) for n, v in cvars)

                clstext = CLS_TPL.format(clsname=cls.__name__, cvars=cvarstext)
                classes.append(clstext)
                exports.append(cls.__name__)

        if exports:
            exportstext = 'export ' + ', '.join(exports)
        else:
            exportstext = ''

        jpsource = MOD_TPL.format(exports=exportstext,
                                  classes='\n\n'.join(classes))

        return jpsource

    def collect_candidate_imports(self):
        imps = super().collect_candidate_imports()

        module = self.module

        for cls in module.__dict__.values():
            if isinstance(cls, configurable.ConfigurableMeta):
                if cls.__module__ != module.__name__:
                    impmod = sys.modules[cls.__module__]
                    if isinstance(impmod, FrontendConfigModule):
                        imps.append(impmod)

        return imps
